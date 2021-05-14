using Microsoft.Spark.Sql;

namespace Listing5_20
{
    class Program
    {
        static void Main(string[] args)
        {
            var spark = SparkSession.Builder().GetOrCreate();
            spark.Range(5).Show();
            spark.Range(10, 12).Show();
        }
    }
}