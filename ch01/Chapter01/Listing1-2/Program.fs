open Microsoft.Spark.Sql

[<EntryPoint>]
let main argv =
    
    let path = argv.[0]
    let spark = SparkSession.Builder().GetOrCreate()
    
    spark.Read().Option("header", "true").Csv(path)
     |> fun dataframe -> dataframe.Filter(Functions.Col("name").EqualTo("Ed Elliott")).Count()
     |> printfn "There are %d row(s)"
    
    0
