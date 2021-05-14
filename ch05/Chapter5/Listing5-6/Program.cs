using System;
using Microsoft.Spark.Sql;

namespace Listing5_6
{
    class Program
    {
        static void Main(string[] args)
        {
            var spark = SparkSession.Builder().GetOrCreate();

            DataFrameReader reader =
                spark.Read().Format("csv").Option("header", true).Option("sep", ",");

            var dataFrame = reader.Load("./csv_file.csv");

            dataFrame.Show();
        }
    }
}