open Microsoft.Spark.Sql
open System

[<EntryPoint>]
let main argv =
    let spark = SparkSession.Builder().GetOrCreate()
    let dataFrame = spark.Range(100L)

    dataFrame.Select(Functions.Column("ID")).Show()
    dataFrame.Select(Functions.Col("ID")).Show()

    dataFrame.Select(Functions.Column("ID").Name("Not ID")).Show()
    dataFrame.Select(Functions.Col("ID").Name("Not ID")).Show()

    dataFrame.Filter(Functions.Column("ID").Gt(100)).Show()
    dataFrame.Filter(Functions.Col("ID").Gt(100)).Show()
    
    0

