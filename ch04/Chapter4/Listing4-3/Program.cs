using System;
using Microsoft.Data.Analysis;
using Microsoft.Spark;
using Microsoft.Spark.Sql;
using FxDataFrame = Microsoft.Data.Analysis.DataFrame;
using static Microsoft.Spark.Sql.Functions;
using static Microsoft.Spark.Sql.DataFrameFunctions;

namespace Listing4_3
{
    class Program
    {
        private static int AddAmount = 100;

        private static Int64DataFrameColumn Add100(Int64DataFrameColumn id)
        {
            return id.Add(AddAmount);
        }

        private static int Add100Pickle(int id)
        {
            return id + AddAmount;
        }

        static void Main(string[] args)
        {
            var spark = SparkSession.Builder().GetOrCreate();

            var dataFrame = spark.Sql("SELECT ID FROM range(1000)");

            //Set shared state to some random number, this will be ignored.
            AddAmount = 991923;

            //Call using Arrow
            var addUdf = VectorUdf<Int64DataFrameColumn, Int64DataFrameColumn>((id) => Add100(id));
            dataFrame.Select(dataFrame["ID"], addUdf(dataFrame["ID"])).Show();

            //Call using Pickling
            var addUdfPickle = Udf<int, int>(id => Add100Pickle(id));
            dataFrame.Select(dataFrame["ID"], addUdfPickle(dataFrame["ID"])).Show();

            //Call using Pickling with anonymous lambda function
            dataFrame.Select(dataFrame["ID"], Udf<int, int>(p => p + AddAmount)(dataFrame["ID"])).Show();
        }
    }
}