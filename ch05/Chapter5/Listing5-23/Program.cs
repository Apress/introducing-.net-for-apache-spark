using System;
using System.Collections.Generic;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;

namespace Listing5_23
{
    class Program
    {
        static void Main(string[] args)
        {
            var spark = SparkSession.Builder().GetOrCreate();
            var dataFrame = spark.CreateDataFrame(new List<GenericRow>()
                {
                    new GenericRow(new object[] {"UK", 2020, 500}),
                    new GenericRow(new object[] {"UK", 2020, 1000}),
                    new GenericRow(new object[] {"FRANCE", 2020, 500}),
                    new GenericRow(new object[] {"FRANCE", 1990, 100}),
                    new GenericRow(new object[] {"UK", 1990, 100})
                },
                new StructType(
                    new List<StructField>()
                    {
                        new StructField("Country", new StringType()),
                        new StructField("Year", new IntegerType()),
                        new StructField("Amount", new IntegerType())
                    }
                ));

            dataFrame.Write().PartitionBy("Year", "Country").Csv("output.csv");

        }
    }
}