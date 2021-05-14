open Microsoft.Spark.Extensions.Delta.Tables
open System.IO
open Microsoft.Spark.Sql
open Microsoft.Spark.Sql.Types

let dropIgnoredColumns (dataFrameToDropColumns:DataFrame) : DataFrame = 
        
        let header = dataFrameToDropColumns.Filter(Functions.Col("index").EqualTo(1)).Collect()
        
        let shouldDropColumn (_:int, data:obj) =
            match data with
                | null -> true
                | _ -> match data.ToString() with
                            | "Date" -> false
                            | "Expense Type" -> false
                            | "Expense Area" -> false
                            | "Supplier" -> false
                            | "Reference" -> false
                            | "Amount" -> false
                            | "index" -> false
                            | null -> true
                            | _ -> true
                            
        let dropColumns =
            let headerRow = header |> Seq.cast<Row> |> Seq.head
            headerRow
                |> fun row -> row.Values
                |> Seq.indexed
                |> Seq.filter shouldDropColumn
                |> Seq.map fst
                |> Seq.map(fun i -> "_c" + i.ToString())
                |> Seq.toArray
        
        dataFrameToDropColumns.Drop dropColumns
        
let getHeaderRow (dataFrame:DataFrame) =
    dataFrame.Filter(Functions.Col("index").EqualTo(1)).Drop("index").Collect()
    
let convertHeaderRowIntoArrayOfNames(columnNames) =
        columnNames
            |> Seq.cast<Row>
            |> Seq.head
            |> fun row -> row.Values
            |> Seq.map(fun i -> i.ToString())
            |> Seq.toArray
            |> Array.map (fun item -> item.Replace(" ", "_"))
            
let fixColumnHeaders (dataFrame:DataFrame) : DataFrame =
    let header = getHeaderRow dataFrame
                    |> convertHeaderRowIntoArrayOfNames
                      
    dataFrame.Filter(Functions.Col("index").Gt(1)).Drop("index").ToDF(header)
   
let filterOutEmptyRows (dataFrame:DataFrame) : DataFrame =
    dataFrame.Filter(Functions.Col("Reference").IsNotNull()).Filter(Functions.Col("Supplier").IsNotNull())
    
    
let fixDateColumn (dataFrame:DataFrame) : DataFrame =
    dataFrame.WithColumn("__Date", Functions.Col("Date"))
     |> fun d -> d.WithColumn("Date", Functions.ToDate(Functions.Col("Date"), "MMMM yyyy"))
     |> fun d-> match d.Filter(Functions.Col("Date").IsNotNull()).Count() with
                    | 0L -> d.WithColumn("Date", Functions.ToDate(Functions.Col("__Date"), "MMM-yy"))
                    | _ -> d
     |> fun d -> d.Drop("__Date")
     
let fixAmountColumn (dataFrame:DataFrame) : DataFrame =
    dataFrame.WithColumn("Amount", Functions.RegexpReplace(Functions.Col("Amount"), "[£,]", ""))
    |> fun d -> d.WithColumn("Amount", Functions.Col("Amount").Cast("float"))
    
let getData(spark:SparkSession, path:string) =
    let readOptions =
        let options = [
            ("inferSchema","true")
            ("header","false")
            ("encoding","ISO-8859-1")
            ("locale","en-GB")
            ("quote","\"")
            ("ignoreLeadingWhiteSpace","true")
            ("ignoreTrailingWhiteSpace","true")
            ("dateFormat","M y")
        ]
        System.Linq.Enumerable.ToDictionary(options, fst, snd)

    spark.Read().Format("csv").Options(readOptions).Load(path)
        |> fun data -> data.WithColumn("index", Functions.MonotonicallyIncreasingId())
        |> dropIgnoredColumns
        |> fixColumnHeaders
        |> filterOutEmptyRows
        |> fixDateColumn
        |> fixAmountColumn
        
        
let writeToStructured(dataFrame:DataFrame, path:string) : unit =
    dataFrame.Write().Mode("overwrite").Format("parquet").Save(path)

let expectedSchema = StructType(
                                   [|
                                       StructField("Date", DateType())
                                       StructField("Expense_Type", StringType())
                                       StructField("Expense_Area", StringType())
                                       StructField("Supplier", StringType())
                                       StructField("Reference", StringType())
                                       StructField("Amount", FloatType())
                                   |]
                               )

let validateSchema (dataFrame:DataFrame) = dataFrame.Schema().Json = expectedSchema.Json

let validateHaveSomeNonNulls (dataFrame:DataFrame) = dataFrame.Filter(Functions.Col("Date").IsNotNull()).Count() > 0L

let validateAmountsPerSupplierGreater25K (dataFrame:DataFrame) = dataFrame.GroupBy(Functions.Col("Supplier")).Sum("Amount").Filter(Functions.Col("Sum(Amount)").Lt(25000)).Count() = 0L

let validateEntity (dataFrame:DataFrame) =
    validateSchema dataFrame
    && validateHaveSomeNonNulls dataFrame
    && validateAmountsPerSupplierGreater25K dataFrame

let writeToCurated (dataFrame:DataFrame, path:string) : unit =
    dataFrame.WithColumn("year", Functions.Year(Functions.Col("Date")))
    |> fun data -> data.WithColumn("month", Functions.Month(Functions.Col("Date")))
    |> fun data -> data.Write().PartitionBy("year", "month").Mode("overwrite").Parquet(path);
    
let writeToFailed (dataFrame:DataFrame, path:string) : unit =
    dataFrame.Write().Mode("overwrite").Parquet(path)

let saveSuppliers (spark: SparkSession, dataFrame:DataFrame, source:string, target:string) =
    
    let suppliers = dataFrame.Select(Functions.Col("Supplier")).Distinct()
    
    match Directory.Exists(sprintf "%s-suppliers" target) with
        | true -> let existingSuppliers = spark.Read().Format("delta").Load(sprintf "%s-suppliers" target)
                  existingSuppliers.Join(suppliers, existingSuppliers.Col("Supplier").EqualTo(suppliers.Col("Supplier")), "left_anti")
                    |> fun newSuppliers -> newSuppliers.WithColumn("Supplier_Hash", Functions.Hash(Functions.Col("Supplier"))).Write().Mode(SaveMode.Append).Format("delta").Save(sprintf "%s-suppliers" target)
         | false -> suppliers.WithColumn("Supplier_Hash", Functions.Hash(Functions.Col("Supplier"))).Write().Format("delta").Save(sprintf "%s-suppliers" target)

let saveExpenseType (spark: SparkSession, dataFrame:DataFrame, source:string, target:string) =
    
    let expenseType = dataFrame.Select(Functions.Col("Expense_Type")).Distinct()
    
    match Directory.Exists(sprintf "%s-expense-type" target) with
        | true -> let existingExpenseType = spark.Read().Format("delta").Load(sprintf "%s-expense-type" target)
                  existingExpenseType.Join(expenseType, existingExpenseType.Col("Expense_Type").EqualTo(expenseType.Col("Expense_Type")), "left_anti")
                    |> fun newExpenseType -> newExpenseType.WithColumn("Expense_Type_Hash", Functions.Hash(Functions.Col("Expense_Type"))).Write().Mode(SaveMode.Append).Format("delta").Save(sprintf "%s-expense-type" target)
         | false -> expenseType.WithColumn("Expense_Type_Hash", Functions.Hash(Functions.Col("Expense_Type"))).Write().Mode("overwrite").Format("delta").Save(sprintf "%s-expense-type" target)
                    

let writeExpenses (dataFrame:DataFrame, target:string) =
    
    let data = dataFrame.WithColumn("Expense_Type_Hash", Functions.Hash(Functions.Col("Expense_Type"))).Drop("Expense_Type")
                |> fun data -> data.WithColumn("Supplier_Hash", Functions.Hash(Functions.Col("Supplier"))).Drop("Supplier").Alias("source")
                
    match Directory.Exists(target) with
        | false -> data.Write().Format("delta").Save(target)
        | true ->  DeltaTable.ForPath(target).Alias("target").Merge(data, "source.Date = target.Date AND source.Expense_Type_Hash = target.Expense_Type_Hash AND source.Expense_Area = target.Expense_Area AND source.Supplier_Hash = target.Supplier_Hash AND source.Reference = target.Reference")
                    |> fun merge -> let options = System.Linq.Enumerable.ToDictionary(["Amount", data.["Amount"]], fst, snd)
                                    merge.WhenMatched("source.Amount != target.Amount").Update(options)
                    |> fun merge -> merge.WhenNotMatched().InsertAll()
                    |> fun merge -> merge.Execute()

let writeToPublished (spark: SparkSession, source:string, target:string) : unit =
    
    let data = spark.Read().Parquet(source)
    saveSuppliers(spark, data, source, target)
    saveExpenseType(spark, data, source, target)
    writeExpenses(data, target)
    
type args = {dataLakePath: string; path: string; year: string; month: string; success: bool}

[<EntryPoint>]
let main argv =
    
    let args = match argv with
                | [|dataLakePath; path; year; month|] -> {dataLakePath = argv.[0]; path = argv.[1]; year = argv.[2]; month = argv.[3]; success = true}
                | _ -> {success = false; dataLakePath= ""; path = ""; year = ""; month = "";}

    match args.success with
        | false ->
            printfn "Error, incorrect args. Expecting 'Data Lake Path' 'file path' 'year' 'month', got: %A" argv
            -1

        | true ->
                  let spark = SparkSession.Builder().Config("spark.sql.sources.partitionOverwriteMode", "dynamic").Config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").GetOrCreate()
                  let data = getData(spark, args.path)
                  
                  writeToStructured (data, (sprintf "%s/structured/%s/%s/%s/%s" args.dataLakePath "ofgem" "over25kexpenses" args.year args.month))
                  match validateEntity data with
                    | false -> writeToFailed(data, (sprintf "%s/failed/%s/%s" args.dataLakePath "ofgem" "over25kexpenses"))
                               -2
                    | true -> writeToCurated(data, (sprintf "%s/curated/%s/%s" args.dataLakePath "ofgem" "over25kexpenses"))
                              writeToPublished(spark,  (sprintf "%s/curated/%s/%s" args.dataLakePath "ofgem" "over25kexpenses"), (sprintf "%s/publish/%s/%s" args.dataLakePath "ofgem" "over25kexpenses"))
                              0