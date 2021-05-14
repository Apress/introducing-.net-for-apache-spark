open System
open System.Collections
open Microsoft.Spark.ML.Feature
open Microsoft.Spark.Sql
open Microsoft.Spark.Sql.Types
open Razorvine.Pickle
open System.Xml

let createXmlDoc(path: string) =
    let doc = XmlDocument()
    doc.Load(path)
    doc
    
let parseXml(doc: XmlDocument) =
    let selectSingleNode node =
        Option.ofObj (doc.SelectSingleNode(node))
        
    let documentTitle =
        match selectSingleNode "//title" with
        | Some node -> node.InnerText
        | None -> doc.SelectSingleNode("//personae[@playtitle]").Attributes.["playtitle"].Value
        
    match selectSingleNode "//play" with
    | Some node -> GenericRow([|documentTitle; node.InnerText|])
    | None -> GenericRow([|documentTitle; doc.SelectSingleNode("//poem").InnerText|])

            
let getDocuments path = System.IO.Directory.GetFiles(path, "*.xml")
                                                |> Seq.map (fun doc -> createXmlDoc doc)
                                                |> Seq.map (fun xml -> parseXml xml)

            
let schema = StructType([|StructField("title", StringType());StructField("content", StringType())|])            


type args = {documentsPath: string; searchTerm: string; success: bool}

let calcNormUDF = Functions.Udf<Row, double>(fun row -> row.Values.[3] :?> ArrayList
                                                     |> Seq.cast
                                                     |> Seq.map (fun item -> item * item)
                                                     |> Seq.sum
                                                     |> Math.Sqrt)


type sparseVector = {items:double; length:double; indices:ArrayList; values:ArrayList}

let cosineSimilarity (vectorA:Row, vectorB:Row, normA:double, normB:double):double =
    
    let indicesA = vectorA.Values.[2]  :?> ArrayList
    let valuesA = vectorA.Values.[3] :?> ArrayList
    
    let indicesB = vectorB.Values.[2] :?> ArrayList
    let valuesB = vectorB.Values.[3] :?> ArrayList
    
    let indexedA = indicesA |> Seq.cast |> Seq.indexed
    let indexedB = indicesB |> Seq.cast |> Seq.indexed |> Seq.map (fun item -> (snd item, fst item)) |> Map.ofSeq
    
    PrettyPrint.print indexedB
    
    let findIndex value = match indexedB.ContainsKey value with
                            | true -> indexedB.[value]
                            | false -> -1
  
    let findValue indexA =
                            let index =  findIndex indexA
                            
                            match index with
                                | -1 -> 0.0
                                | _ -> unbox<double> (valuesB.Item(unbox<int> (index))) 
    
    let dotProduct = indexedA
                       |> Seq.map (fun index -> (unbox<double>valuesA.[fst index]) * (findValue (unbox<int> indicesA.[fst index])))
                       |> Seq.sum
    
    normA * normB |> fun divisor -> match divisor with
                                                | 0.0 -> 0.0
                                                | _ -> dotProduct / divisor 
    

let cosineSimilarityUDF = Functions.Udf<Row, Row, double, double, double>(fun vectorA vectorB normA normB -> cosineSimilarity(vectorA, vectorB, normA, normB))

[<EntryPoint>]
let main argv =
    let args = match argv with
                | [|documentPath; searchTerm|] -> {documentsPath = argv.[0]; searchTerm = argv.[1]; success = true}
                | _ -> {success = false; documentsPath = ""; searchTerm = ""}

    match args.success with
        | false ->
            printfn "Error, incorrect args. Expecting 'Path to documents' 'search term', got: %A" argv
            -1
    
        | true ->
            let spark = SparkSession.Builder().GetOrCreate()
            let documents = getDocuments args.documentsPath
            
            let documents = spark.CreateDataFrame(documents, StructType([|StructField("title", StringType());StructField("content", StringType())|]))
            
            let tokenizer = Tokenizer().SetInputCol("content").SetOutputCol("words")
            let hashingTF = HashingTF().SetInputCol("words").SetOutputCol("rawFeatures").SetNumFeatures(1000000)
            let idf = IDF().SetInputCol("rawFeatures").SetOutputCol("features")
            
            let featurized = tokenizer.Transform documents
                                |> hashingTF.Transform
                                
                               
            
            let model = featurized
                        |> idf.Fit
                        
            let normalizedDocuments = model.Transform featurized
                                            |> fun data -> data.Select(Functions.Col("features"), calcNormUDF.Invoke(Functions.Col("features")).Alias("norm"), Functions.Col("title"))
                                            
            normalizedDocuments.Show(100, 100)                                            
            let term = GenericRow([|"Montague and capulets"|])
            let searchTerm = spark.CreateDataFrame([|term|], StructType([|StructField("content", StringType())|]) )
            
            tokenizer.Transform searchTerm
                |> hashingTF.Transform
                |> model.Transform
                |> fun data -> data.WithColumnRenamed("features", "searchTermFeatures")
                |> fun data -> data.WithColumn("searchTermNorm", calcNormUDF.Invoke(Functions.Col("searchTermFeatures")))
                |> normalizedDocuments.CrossJoin
                |> fun data -> data.WithColumn("similarity", cosineSimilarityUDF.Invoke(Functions.Col("features"), Functions.Col("searchTermFeatures"), Functions.Col("norm"), Functions.Col("searchTermNorm")))
                |> fun matched -> matched.OrderBy(Functions.Desc("similarity")).Select("title", "similarity")
                |> fun ordered -> ordered.Show(100, 1000)
            0       
    0