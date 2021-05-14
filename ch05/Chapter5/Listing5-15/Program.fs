open Microsoft.Spark.Sql
open Microsoft.Spark.Sql.Types
open System

[<EntryPoint>]
let main argv =
    
    let dataFrame = SparkSession.Builder().GetOrCreate()
                    |> fun spark -> spark.Read()
                    |> fun reader ->
                        reader.Schema
                            (StructType
                                ([| StructField("greeting", StringType())
                                    StructField("first_number", IntegerType())
                                    StructField("second_number", FloatType()) |]))
                    
                    |> fun reader -> reader.Option("sep", ",").Option("header", "false").Csv("csv_file.csv")
        
    dataFrame.PrintSchema()
    dataFrame.Show()

    0
