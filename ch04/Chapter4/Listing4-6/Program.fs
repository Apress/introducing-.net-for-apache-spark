open Microsoft.Spark.Sql
open System

[<EntryPoint>]
let main argv =
    let spark = SparkSession.Builder().GetOrCreate()
    spark.Udf().RegisterJavaUDAF("java_function", "com.company.ClassName")
    let dataFrame = spark.Sql("SELECT ID, java_function(ID) as java_function_output FROM range(1000)")
    dataFrame.Select(Microsoft.Spark.Sql.Functions.CallUDF("java_udf", dataFrame.["ID"])).Show()
    
    0
