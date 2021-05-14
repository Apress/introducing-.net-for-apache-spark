using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using static Microsoft.Spark.Sql.Functions;

namespace Listing9_1
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            var spark = SparkSession.Builder().GetOrCreate();

            var rawDataFrame = spark.ReadStream().Format("kafka")
                .Option("kafka.bootstrap.servers", "localhost:9092")
                .Option("subscribe", "sql.dbo.SalesOrderItems")
                .Option("startingOffset", "earliest").Load();

            var messageSchema = new StructType(
                new List<StructField>
                {
                    new StructField("schema", new StringType()),
                    new StructField("payload", new StructType(
                        new List<StructField>
                        {
                            new StructField("after", new StructType(
                                new List<StructField>
                                {
                                    new StructField("Order_ID", new IntegerType()),
                                    new StructField("Product_ID", new IntegerType()),
                                    new StructField("Amount", new IntegerType()),
                                    new StructField("Price_Sold", new FloatType()),
                                    new StructField("Margin", new FloatType())
                                })),
                            new StructField("source", new StructType(new List<StructField>
                            {
                                new StructField("version", new StringType()),
                                new StructField("ts_ms", new LongType())
                            }))
                        }))
                }
            );

            var parsedDataFrame = rawDataFrame
                .SelectExpr("CAST(value as string) as value")
                .WithColumn("new", FromJson(Col("value"), messageSchema.Json))
                .Select("value", "new.payload.after.*", "new.payload.source.*")
                .WithColumn("timestamp", Col("ts_ms").Divide(1000).Cast("timestamp"));

            var totalByProductSoldLast5Minutes = parsedDataFrame.WithWatermark("timestamp", "30 seconds")
                .GroupBy(Window(Col("timestamp"), "5 minute"), Col("Product_ID")).Sum("Price_Sold")
                .WithColumnRenamed("sum(Price_Sold)", "Total_Price_Sold_Per_5_Minutes")
                .WriteStream()
                .Format("parquet")
                .Option("checkpointLocation", "/tmp/checkpointLocation")
                .OutputMode("append")
                .Option("path", "/tmp/ValueOfProductsSoldPer5Minutes")
                .Start();

            var operationalAlerts = parsedDataFrame
                .WriteStream()
                .Format("console")
                .ForeachBatch((df, id) => HandleStream(df, id))
                .Start();

            Task.WaitAll(
                Task.Run(() => operationalAlerts.AwaitTermination()),
                Task.Run(() => totalByProductSoldLast5Minutes.AwaitTermination())
            );
        }

        private static void HandleStream(DataFrame df, in long batchId)
        {
            var tooLowMargin = df.Filter(Col("Margin").Lt(0.10));

            if (tooLowMargin.Count() > 0)
            {
                tooLowMargin.Show();
                Console.WriteLine("Trigger Ops Alert Here");
            }
        }
    }
}