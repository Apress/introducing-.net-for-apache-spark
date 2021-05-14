using System;
using Microsoft.Spark.Sql;

namespace IntroducingSparkDotNet
{
    class Program
    {
        static void Main(string[] args)
        {
            var spark = SparkSession
                .Builder()
                .AppName("DemoApp")
                .GetOrCreate();

            var dataFrame = spark.Sql("select id, rand() as random_number from range(1000)");

            dataFrame
                .Write()
                .Format("csv")
                .Option("header", true)
                .Option("sep", "|")
                .Mode("overwrite")
                .Save(args[0]);

            foreach (var row in dataFrame.Collect())
            {
                if (row[0] as int? % 2 == 0)
                {
                    Console.WriteLine($"line: {row[0]}");
                }
            }
        }
    }
}