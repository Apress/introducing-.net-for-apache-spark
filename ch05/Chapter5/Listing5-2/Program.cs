using System;
using Microsoft.Spark.Sql;
using static Microsoft.Spark.Sql.Functions;

namespace Listing5_2
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");
            var regex = @"^([\d.]+) (\S+) (\S+) \[([\w\d:/]+\s[+\-]\d{4})\] ""(.+?)"" (\d{3}) ([\d\-]+) ""([^""]+)"" ""([^""]+)"".*";
            var spark = SparkSession.Builder().AppName("LogReader").GetOrCreate();
            var dataFrame = spark.Read().Text("log.txt");

            dataFrame
                .WithColumn("user", RegexpExtract(dataFrame["value"], regex, 3))
                .WithColumn("bytes", RegexpExtract(dataFrame["value"], regex, 7).Cast("int"))
                .WithColumn("uri", RegexpExtract(dataFrame["value"], regex, 5))
                .Drop("value")
                .GroupBy("user", "uri")
                .Agg(Sum("bytes").Alias("TotalBytesPerUser"), Count("user").Alias("RequestsPerUser"))
                .Show();
        }
    }
}