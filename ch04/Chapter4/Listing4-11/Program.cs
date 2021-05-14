using Apache.Arrow;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using ArrowStringType = Apache.Arrow.Types.StringType;
using FloatType = Microsoft.Spark.Sql.Types.FloatType;
using StringType = Microsoft.Spark.Sql.Types.StringType;
using StructType = Microsoft.Spark.Sql.Types.StructType;

namespace Listing4_11
{
    class Program
    {
        static void Main(string[] args)
        {
            var spark = SparkSession.Builder().GetOrCreate();

            var dataFrame = spark.Sql(
                "SELECT 'Ed' as Name, 'Sandwich' as Purchase, 4.95 as Cost UNION ALL SELECT 'Sarah', 'Drink', 2.95 UNION ALL SELECT 'Ed', 'Chips', 1.99 UNION ALL SELECT 'Ed', 'Drink', 3.45  UNION ALL SELECT 'Sarah', 'Sandwich', 8.95");

            dataFrame = dataFrame.WithColumn("Cost", dataFrame["Cost"].Cast("Float"));

            dataFrame.Show();
            var allowableExpenses = dataFrame.GroupBy("Name").Apply(new StructType(new[]
                {
                    new StructField("Name", new StringType()),new StructField("TotalCostOfAllowableExpenses", new FloatType())
                }), TotalCostOfAllowableExpenses
            );

            allowableExpenses.PrintSchema();
            allowableExpenses.Show();
        }

        private static RecordBatch TotalCostOfAllowableExpenses(RecordBatch records)
        {
            var purchaseColumn = records.Column("Purchase") as StringArray;
            var costColumn = records.Column("Cost") as FloatArray;

            float totalCost = 0F;

            for (int i = 0; i < purchaseColumn.Length; i++)
            {
                var cost = costColumn.GetValue(i);

                var purchase = purchaseColumn.GetString(i);

                if (purchase != "Drink" && cost.HasValue)
                    totalCost += cost.Value;
            }

            int returnLength = records.Length > 0 ? 1 : 0;

            return new RecordBatch(
                new Schema.Builder()
                    .Field(f => f.Name("Name").DataType(ArrowStringType.Default))
                    .Field(f => f.Name("TotalCostOfAllowableExpenses").DataType(Apache.Arrow.Types.FloatType.Default))
                    .Build(),
                new IArrowArray[]
                {
                    records.Column("Name"),
                    new FloatArray.Builder().Append(totalCost).Build()
                }, returnLength);
        }
    }
}