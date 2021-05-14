using System;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;

namespace Listing6_03
{
    class Program
    {
        static void Main(string[] args)
        {
            var spark = SparkSession.Builder().Config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true").GetOrCreate();

            var dataFrame = spark.CreateDataFrame(new[] { 10, 11, 12, 13, 14, 15 }).WithColumnRenamed("_1", "ID");

            dataFrame.Write().Mode("overwrite").SaveAsTable("saved_table");
            spark.Sql("select * from saved_table").Show();
        }
    }
}