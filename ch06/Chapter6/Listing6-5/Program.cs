using System;
using Microsoft.Spark.Sql;

namespace Listing6_5
{
    class Program
    {
        static void Main(string[] args)
        {
            var spark = SparkSession.Builder().GetOrCreate();

            var dataFrame = spark.CreateDataFrame(new[] { 10, 11, 12, 13, 14, 15 }).WithColumnRenamed("_1", "ID");

            dataFrame.CreateTempView("temp_view");
            Console.WriteLine("select * from temp_view:");
            spark.Sql("select * from temp_view").Show();

            dataFrame.CreateOrReplaceTempView("temp_view");
            Console.WriteLine("select * from temp_view:");
            spark.Sql("select * from temp_view").Show();

            dataFrame.CreateGlobalTempView("global_temp_view");
            Console.WriteLine("select * from global_temp.global_temp_view:");
            spark.Sql("select * from global_temp.global_temp_view").Show();

            dataFrame.CreateOrReplaceGlobalTempView("global_temp_view");
            Console.WriteLine("select * from global_temp.global_temp_view:");
            spark.Sql("select * from global_temp.global_temp_view").Show();
        }
    }
}