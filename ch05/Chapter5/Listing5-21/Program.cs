using Microsoft.Spark.Sql;

namespace Listing5_21
{
    class Program
    {
        static void Main(string[] args)
        {
            var spark = SparkSession.Builder().GetOrCreate();
            var dataFrame = spark.Range(100);

            dataFrame.Write().Csv("output.csv");
            dataFrame.Write().Format("json").Save("output.json");
        }
    }
}