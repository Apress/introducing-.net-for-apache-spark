using System;
using Microsoft.Spark.Sql;
using static Microsoft.Spark.Sql.Functions;

namespace Listing4_5
{
    class Program
    {
        static void Main(string[] args)
        {
            var spark = SparkSession.Builder().GetOrCreate();

            spark.Udf().RegisterJavaUDAF("java_function", "com.company.ClassName");

            var dataFrame = spark.Sql("SELECT ID, java_function(ID) as java_function_output FROM range(1000)");
            dataFrame.Select(CallUDF("java_udf", dataFrame["ID"])).Show();
        }
    }
}