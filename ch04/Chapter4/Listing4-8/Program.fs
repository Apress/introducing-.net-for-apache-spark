open Microsoft.Spark.Sql

[<EntryPoint>]
let main argv =
    let spark = SparkSession.Builder().GetOrCreate()
    let dataFrame = spark.Sql("SELECT ID FROM range(1000)")
    let add100 = Functions.Udf<System.Nullable<int>, int>(fun input -> if input.HasValue then input.Value + 100 else 100 )
    dataFrame.Select(add100.Invoke(dataFrame.["ID"])).Show()
    0
    

