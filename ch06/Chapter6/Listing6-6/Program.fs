open Microsoft.Spark.Sql
open System

[<EntryPoint>]
let main argv =
    let spark = SparkSession.Builder().GetOrCreate()
    
    let dataFrame = spark.CreateDataFrame([|10;11;12;13;14;15|]).WithColumnRenamed("_1", "ID")
    
    dataFrame.CreateTempView("temp_view")
    printfn "select * from temp_view:"
    spark.Sql("select * from temp_view").Show()
    
    dataFrame.CreateOrReplaceTempView("temp_view")
    printfn "select * from temp_view:"
    spark.Sql("select * from temp_view").Show()

    dataFrame.CreateGlobalTempView("global_temp_view")
    printfn "select * from global_temp.global_temp_view:"
    spark.Sql("select * from global_temp.global_temp_view").Show()
    
    dataFrame.CreateOrReplaceGlobalTempView("global_temp_view")
    printfn "select * from global_temp.global_temp_view:"
    spark.Sql("select * from global_temp.global_temp_view").Show()

    0
