using Microsoft.Spark.Sql;

namespace Listing5_22
{
    class Program
    {
        static void Main(string[] args)
        {
            var spark = SparkSession.Builder().GetOrCreate();
            var dataFrame = spark.Range(100);

            dataFrame.Write().Mode("overwrite").Csv("output.csv");
            dataFrame.Write().Mode("ignore").Csv("output.csv");
            dataFrame.Write().Mode("append").Csv("output.csv");
            dataFrame.Write().Mode("error").Csv("output.csv");

        }
    }
}