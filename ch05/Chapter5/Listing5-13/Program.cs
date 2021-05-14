using System;
using Microsoft.Spark.Sql;

namespace Listing5_13
{
    class Program
    {
        static void Main(string[] args)
        {
            var spark = SparkSession.Builder().GetOrCreate();
            var dataFrame = spark.Read().Option("sep", ",").Option("header", "false")
                .Schema("greeting string, first_number int, second_number float")
                .Csv("csv_file.csv");

            dataFrame.PrintSchema();
            dataFrame.Show();
        }
    }
}