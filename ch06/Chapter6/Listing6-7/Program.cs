using System;
using Microsoft.Spark.Sql;

namespace Listing6_7
{
    class Program
    {
        static void Main(string[] args)
        {
            var spark = SparkSession.Builder().GetOrCreate();
            spark.Sql("CREATE DATABASE InputData");

            spark.Catalog.SetCurrentDatabase("InputData");
            spark.Catalog.CreateTable("id_list", "./ID.parquet");

            var tables = spark.Catalog.ListTables("InputData");

            foreach (var row in tables.Collect())
            {
                var name = row[0].ToString();
                var database = row[1].ToString();

                Console.WriteLine($"Database: {database}, Table: {name}");
                var table = spark.Catalog.ListColumns(database, name);
                foreach (var column in table.Collect())
                {
                    var columnName = column[0].ToString();
                    var dataType = column[2].ToString();

                    Console.WriteLine($"{columnName}\t{dataType}");
                }
            }
        }
    }
}