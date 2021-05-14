open Microsoft.Spark.Sql
open Microsoft.Spark.Sql.Types


[<EntryPoint>]
let main argv =

    let spark = SparkSession.Builder().GetOrCreate()

    spark.CreateDataFrame([| "a"; "b"; "c" |]).Show()

    spark.CreateDataFrame([| true; true; false |]).Show()

    spark.CreateDataFrame([| GenericRow([| "hello"; 123; 543.0 |])
                             GenericRow([| "hi"; 987; 456.0 |]) |],
                          StructType
                              ([| StructField("greeting", StringType())
                                  StructField("first_number", IntegerType())
                                  StructField("second_number", DoubleType()) |]

                              )).Show()

    0
