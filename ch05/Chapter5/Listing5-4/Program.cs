using System;
using Microsoft.Spark.Sql;

namespace Listing5_4
{
    class Program
    {
        static void Main(string[] args)
        {
            var spark = SparkSession.Builder().GetOrCreate();
            spark.Sql("select assert_true(false)").Show();
        }
    }
}