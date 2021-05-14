using Microsoft.Spark.Sql;

namespace Listing5_19
{
    class Program
    {
        static void Main(string[] args)
        {
            var spark = SparkSession.Builder().GetOrCreate();

            spark.Sql("SELECT ID FROM Range(100, 150)").Show();

            spark.Sql("SELECT 'Hello' as Greeting, 123 as A_Number").Show();

            spark.Sql("SELECT 'Hello' as Greeting, 123 as A_Number union SELECT 'Hi', 987").Show();
        }
    }
}