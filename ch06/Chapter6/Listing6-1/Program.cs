using System;
using Microsoft.Spark.Sql;

namespace Listing6_01
{
    class Program
    {
        static void Main(string[] args)
        {
            var spark = SparkSession.Builder().GetOrCreate();
            spark.Sql("CREATE TABLE Users USING csv OPTIONS (path './Names.csv')");

            spark.Sql("SELECT * FROM Users").Show();
        }
    }
}