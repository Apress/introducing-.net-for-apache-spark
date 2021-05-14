using System;
using Microsoft.Spark.Sql;
using static Microsoft.Spark.Sql.Functions;

namespace TransformingData_CSharp
{
    class Program
    {
        static void Main(string[] args)
        {
            var spark = SparkSession
                .Builder()
                .GetOrCreate();

            var data = spark.Range(100).WithColumn("Name", Lit("Ed"))
                .Union(spark.Range(100).WithColumn("Name", Lit("Bert")))
                .Union(spark.Range(100).WithColumn("Name", Lit("Lillian")));

            var counts = data.GroupBy(Col("Name")).Count();
            counts.Show();
        }
    }
}