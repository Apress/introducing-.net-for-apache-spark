using Microsoft.Spark.Sql;

namespace Listing5_18
{
    class Program
    {
        static void Main(string[] args)
        {
            var spark = SparkSession.Builder().GetOrCreate();

            spark.CreateDataFrame(new[] { "a", "b", "c" }).WithColumnRenamed("_1", "ColumnName").Show();
        }
    }
}