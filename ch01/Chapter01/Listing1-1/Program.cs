using System;
using System.Linq;
using Microsoft.Spark.Sql;
using static Microsoft.Spark.Sql.Functions;

namespace Introduction_CSharp
{
    class Program
    {
        static void Main(string[] args)
        {
            var path = args.FirstOrDefault();

            var spark = SparkSession
                .Builder()
                .GetOrCreate();

            var dataFrame = spark.Read().Option("header", "true").Csv(path);
            var count = dataFrame.Filter(Col("name") == "Ed Elliott").Count();
            Console.WriteLine($"There are {count} row(s)");
        }
    }
}