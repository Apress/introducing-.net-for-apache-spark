using System;
using System.Collections.Generic;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;

namespace Listing5_14
{
    class Program
    {
        static void Main(string[] args)
        {
            var spark = SparkSession.Builder().GetOrCreate();

            var schema = new StructType(new List<StructField>()
            {
                new StructField("greeting", new StringType()),
                new StructField("first_number", new IntegerType()),
                new StructField("second_number", new FloatType())
            });

            var dataFrame = spark.Read().Option("sep", ",").Option("header", "false")
                .Schema(schema)
                .Csv("csv_file.csv");

            dataFrame.PrintSchema();
            dataFrame.Show();
        }
    }
}