using Microsoft.Spark.Sql;


namespace Listing3_6
{
    class Program
    {
        static void RunBasicDataFrameExample(SparkSession spark)
        {

        }

        static void Main(string[] args)
        {
            var spark = SparkSession
                .Builder()
                .AppName("Spark SQL basic example")
                .Config("spark.some.config.option", "some-value")
                .GetOrCreate();

            RunBasicDataFrameExample(spark);
        }
    }
}