open Microsoft.Spark.Sql
open System

[<EntryPoint>]
let main argv =

    let spark = SparkSession.Builder().GetOrCreate()
    let udfIntToString = Microsoft.Spark.Sql.Functions.Udf<int, string>(fun (id) -> "The id is " + id.ToString())
    let dataFrame = spark.Sql("SELECT ID from range(1000)")
    dataFrame.Select(udfIntToString.Invoke(dataFrame.["ID"])).Show()
    0