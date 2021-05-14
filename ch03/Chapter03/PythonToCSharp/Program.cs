using System;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;

namespace Listing3_6
{
    class Program
    {
        static void BasicDfExample(SparkSession spark)
        {
            var dataFrame = spark.Read().Json("/Users/ed/spark-2.4.6-bin-without-hadoop/examples/src/main/resources/people.json");
            dataFrame.Show();

            dataFrame.PrintSchema();

            dataFrame.Select("name").Show();

            dataFrame.Select(dataFrame["name"], dataFrame["age"] + 1).Show();
            dataFrame.Select(dataFrame["name"], dataFrame["age"].Plus(1)).Show();

            dataFrame.Filter(dataFrame["age"] > 21).Show();
            dataFrame.Filter(dataFrame["age"].Gt(21)).Show();

            dataFrame.GroupBy(dataFrame["age"]).Count().Show();

            dataFrame.CreateOrReplaceTempView("people");
            var sqlDataFrame = spark.Sql("SELECT * FROM people");

            dataFrame.CreateGlobalTempView("people");
            spark.Sql("SELECT * FROM global_temp.people").Show();
            spark.NewSession().Sql("SELECT * FROM global_temp.people").Show();
        }

        static void Main(string[] args)
        {
            var spark = SparkSession.Builder().GetOrCreate();
            BasicDfExample(spark);
        }
    }
}