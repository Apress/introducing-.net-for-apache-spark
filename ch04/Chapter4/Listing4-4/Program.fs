open Microsoft.Data.Analysis
open Microsoft.Spark.Sql
open System
let mutable SharedState = 100 

[<EntryPoint>]
let main argv =
    
    let spark = SparkSession.Builder().GetOrCreate()
    
    let dataFrame = spark.Sql("SELECT ID FROM range(1000)")
    SharedState = 991923
    let addUdf =  Microsoft.Spark.Sql.DataFrameFunctions.VectorUdf<Int64DataFrameColumn, Int64DataFrameColumn>(fun (column) -> column.Add(SharedState))
    dataFrame.Select(dataFrame.["ID"], addUdf.Invoke(dataFrame.["ID"])).Show()
    
    0
    