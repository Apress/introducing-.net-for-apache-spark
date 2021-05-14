using System;
using System.Diagnostics;
using Microsoft.Spark.Sql;
using static Microsoft.Spark.Sql.Functions;

namespace Listing1_1
{
    class Program
    {
        static void Main(string[] args)
        {
            var spark = SparkSession.Builder().GetOrCreate();

            Func<Column, Column> udfIntToString = Udf<int, string>(id => IntToStr(id));

            var dataFrame = spark.Sql("SELECT ID from range(1000)");

            dataFrame.Select(udfIntToString(dataFrame["ID"])).Show();

            string IntToStr(int id)
            {
                Debugger.Break();
                return $"The id is {id}";
            }
        }
    }
}