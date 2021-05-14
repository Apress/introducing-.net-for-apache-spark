using System;
using Apache.Arrow.Types;
using Microsoft.Spark.Sql;

namespace Listing11_7
{
    class Program
    {
        static void Main(string[] args)
        {
            var spark = SparkSession.Builder().Config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .Config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .GetOrCreate();

            spark.Range(100).WithColumn("name", Functions.Lit("Sammy")).Write().Mode("overwrite").Parquet("/tmp/delta-sql-demo");
            spark.Sql("CONVERT TO DELTA parquet.`/tmp/delta-sql-demo");
            spark.Sql("SELECT * FROM delta.`/tmp/delta-sql-demo`").Show();
        }
    }
}