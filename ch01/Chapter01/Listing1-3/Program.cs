using System;
using Microsoft.Spark.Sql;

namespace TransformingData_CSharp
{
    class Program
    {
        static void Main(string[] args)
        {
            var spark = SparkSession
                .Builder()
                .GetOrCreate();

            var filtered = spark.Read().Parquet("1.parquet")
                .Filter(Functions.Col("event_type") == Functions.Lit(999));

            filtered.Write().Mode("overwrite").Parquet("output.parquet");
            Console.WriteLine($"Wrote: {filtered.Count()} rows");
        }
    }
}
