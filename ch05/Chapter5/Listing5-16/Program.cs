using System;
using System.Collections.Generic;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;

namespace Listing5_16
{
    class Program
    {
        static void Main(string[] args)
        {
            var spark = SparkSession.Builder().GetOrCreate();

            spark.CreateDataFrame(new[] { "a", "b", "c" }).Show();
            spark.CreateDataFrame(new[] { true, true, false }).Show();

            var schema = new StructType(new List<StructField>()
            {
                new StructField("greeting", new StringType()),
                new StructField("first_number", new IntegerType()),
                new StructField("second_number", new DoubleType())
            });

            IEnumerable<GenericRow> rows = new List<GenericRow>()
            {
                new GenericRow(new object[] {"hello", 123, 543D}),
                new GenericRow(new object[] {"hi", 987, 456D})
            };

            spark.CreateDataFrame(rows, schema).Show();
        }
    }
}