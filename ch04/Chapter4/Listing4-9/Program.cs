using System;
using Microsoft.Data.Analysis;
using Microsoft.Spark.Sql;
using static Microsoft.Spark.Sql.DataFrameFunctions;

namespace Listing4_9
{
    class Program
    {
        static void Main(string[] args)
        {
            var spark = SparkSession.Builder().GetOrCreate();

            var dataFrame = spark.Sql("SELECT ID FROM range(1000)");

            var add100 = VectorUdf<Int64DataFrameColumn, Int64DataFrameColumn, Int64DataFrameColumn>((first, second) => first.Add(second));

            dataFrame.Select(add100(dataFrame["ID"], dataFrame["ID"])).Show();
        }
    }
}