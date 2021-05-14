using System;
using Microsoft.Spark.Sql;
using static Microsoft.Spark.Sql.Functions;

namespace Listing4_7
{
    class Program
    {
        static void Main(string[] args)
        {
            var spark = SparkSession.Builder().GetOrCreate();

            var dataFrame = spark.Sql("SELECT ID FROM range(1000)");

            var add100 = Udf<int?, int>((input) => input + 100 ?? 100);

            dataFrame.Select(add100(dataFrame["ID"])).Show();
        }
    }
}