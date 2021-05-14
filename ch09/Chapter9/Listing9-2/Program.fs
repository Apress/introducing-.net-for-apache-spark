open Microsoft.Spark.Sql
open Microsoft.Spark.Sql.Types

let messageSchema() =
         StructType(
                      [|
                          StructField("schema", StringType())
                          StructField("payload", StructType(
                                                              [|
                                                                    StructField("after", StructType(
                                                                                                     [|
                                                                                                         StructField("Order_ID", IntegerType())
                                                                                                         StructField("Product_ID", IntegerType())
                                                                                                         StructField("Amount", IntegerType())
                                                                                                         StructField("Price_Sold", FloatType())
                                                                                                         StructField("Margin", FloatType())
                                                                                                     |]
                                                                     ))
                                                                    StructField("source", StructType(
                                                                                                        [|
                                                                                                           StructField("version", StringType())
                                                                                                           StructField("ts_ms", LongType())
                                                                                                        |]
                                                                                                    ))
                                                              |]
                          ))
                      |]
          )

let handleStream (dataFrame:DataFrame, _) : unit  = 
                                                    dataFrame.Filter(Functions.Col("Margin").Lt(0.10))
                                                    |> fun failedRows -> match failedRows.Count() with
                                                                            | 0L -> printfn "We had no failing rows"
                                                                                    failedRows.Show()
                                                                            | _ -> printfn "Trigger Ops Alert Here"
   
[<EntryPoint>]
let main argv =
    
    let spark = SparkSession.Builder().GetOrCreate();

    let rawDataFrame = spark.ReadStream()
                       |> fun stream -> stream.Format("kafka")
                       |> fun stream -> stream.Option("kafka.bootstrap.servers", "localhost:9092")
                       |> fun stream -> stream.Option("subscribe", "sql.dbo.SalesOrderItems")
                       |> fun stream -> stream.Option("startingOffset", "earliest")
                       |> fun stream -> stream.Load()
                       
    rawDataFrame.PrintSchema()

    let parsedDataFrame = rawDataFrame
                            |> fun dataFrame -> dataFrame.SelectExpr("CAST(value as string) as value")
                            |> fun dataFrame -> dataFrame.WithColumn("new", Functions.FromJson(Functions.Col("value"), messageSchema().Json))
                            |> fun dataFrame -> dataFrame.Select("new.payload.after.*", "new.payload.source.*")
                            |> fun dataFrame -> dataFrame.WithColumn("timestamp", Functions.Col("ts_ms").Divide(1000).Cast("timestamp"))
        
    let totalValueSoldByProducts = parsedDataFrame.WithWatermark("timestamp", "30 seconds")
                                    |> fun dataFrame -> dataFrame.GroupBy(Functions.Window(Functions.Col("timestamp"), "5 minute"), Functions.Col("Product_ID")).Sum("Price_Sold")
                                    |> fun dataFrame -> dataFrame.WithColumnRenamed("sum(Price_Sold)", "Total_Price_Sold_Per_5_Minutes")
                                    |> fun dataFrame -> dataFrame.WriteStream()
                                    |> fun stream -> stream.Format("parquet")
                                    |> fun stream -> stream.Option("checkpointLocation", "/tmp/checkpointLocation")
                                    |> fun stream -> stream.OutputMode("append")
                                    |> fun stream -> stream.Option("path", "/tmp/ValueOfProductsSoldPer5Minutes")
                                    |> fun stream -> stream.Start()
    
    let operationalAlerts = parsedDataFrame.WithWatermark("timestamp", "30 seconds")
                                        |> fun dataFrame -> dataFrame.WriteStream()
                                        |> fun stream -> stream.ForeachBatch(fun dataFrame batchId -> handleStream(dataFrame,batchId))
                                        |> fun stream -> stream.Start()
    
    [|
     async {
         operationalAlerts.AwaitTermination()
     }
     async{
        totalValueSoldByProducts.AwaitTermination()
     }
     |]
        |> Async.Parallel
        |> Async.RunSynchronously
        |> ignore
    
    0
