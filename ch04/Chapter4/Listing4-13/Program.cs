using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using Apache.Arrow;
using Microsoft.Data.Analysis;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using Column = Microsoft.Spark.Sql.Column;
using DataFrame = Microsoft.Spark.Sql.DataFrame;
using FxDataFrame = Microsoft.Data.Analysis.DataFrame;
using static Microsoft.Spark.Sql.ExperimentalDataFrameFunctions;

namespace Listing4_13
{
    class Program
    {
        static void Main(string[] args)
        {
            var spark = SparkSession.Builder().GetOrCreate();

            var dataFrame = spark.Range(10000);

            Func<Column, Column> discPrice = VectorUdf<Int64DataFrameColumn, Int64DataFrameColumn>(
                (id) => id.Multiply(100).Divide(2));

            dataFrame.Select(dataFrame["ID"], discPrice(dataFrame["ID"])).Show();
        }

    }
}