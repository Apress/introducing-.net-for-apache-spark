open Microsoft.Spark.Sql
open System

[<EntryPoint>]
let main argv =
    
    let spark = SparkSession.Builder().GetOrCreate()
    let reader = spark.Read()
                |> fun reader -> reader.Format("csv")
                |> fun reader -> reader.Option("header", true)
                |> fun reader -> reader.Option("sep", "|")
                
    let dataFrame = reader.Load("./csv_file.csv")
    dataFrame.Show()
    
    0