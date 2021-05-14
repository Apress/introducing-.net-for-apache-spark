using Microsoft.Spark.Sql;
using static Microsoft.Spark.Sql.Functions;

namespace Listing5_24
{
    class Program
    {
        static void Main(string[] args)
        {
            var spark = SparkSession.Builder().GetOrCreate();
            var dataFrame = spark.Range(100);

            dataFrame.Select(Column("ID")).Show();
            dataFrame.Select(Col("ID")).Show();

            dataFrame.Select(Column("ID").Name("Not ID")).Show();
            dataFrame.Select(Col("ID").Name("Not ID")).Show();

            dataFrame.Filter(Column("ID").Gt(100)).Show();
            dataFrame.Filter(Col("ID").Gt(100)).Show();
        }
    }
}