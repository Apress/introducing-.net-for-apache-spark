using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using Microsoft.Spark.Sql;

namespace Listing8_1
{
    class Program
    {
        static void Main(string[] args)
        {

            if (args.Length != 4)
            {
                Console.WriteLine($"Error, incorrect args. Expecting 'Data Lake Path' 'file path' 'year' 'month', got: {args}");
                return;
            }

            var spark = SparkSession.Builder()
                .Config("spark.sql.sources.partitionOverwriteMode", "dynamic")
                .GetOrCreate();

            var dataLakePath = args[0];
            var sourceFile = args[1];
            var year = args[2];
            var month = args[3];

            const string sourceSystem = "ofgem";
            const string entity = "over25kexpenses";

            ProcessEntity(spark, sourceFile, dataLakePath, sourceSystem, entity, year, month);
        }

        private static void ProcessEntity(SparkSession spark, string sourceFile, string dataLakePath, string sourceSystem, string entity, string year, string month)
        {
            var data = OfgemExpensesEntity.ReadFromSource(spark, sourceFile);

            OfgemExpensesEntity.WriteToStructured(data, $"{dataLakePath}/structured/{sourceSystem}/{entity}/{year}/{month}");

            OfgemExpensesEntity.WriteToCurated(data, $"{dataLakePath}/curated/{sourceSystem}/{entity}");

            OfgemExpensesEntity.WriteToPublish(spark, $"{dataLakePath}/curated/{sourceSystem}/{entity}", $"{dataLakePath}/publish/{sourceSystem}/{entity}");
        }
    }
}