using System.Collections.Generic;
using Microsoft.Spark.Extensions.Delta.Tables;
using Microsoft.Spark.Sql;
using static Microsoft.Spark.Sql.Functions;

namespace Listing11_1
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            var spark = SparkSession.Builder()
                .Config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .GetOrCreate();

            if (!DeltaTable.IsDeltaTable("parquet.`/tmp/delta-demo`"))
            {
                spark.Range(1000).WithColumn("name", Lit("Sammy")).Write().Mode("overwrite")
                    .Parquet("/tmp/delta-demo");
                DeltaTable.ConvertToDelta(spark, "parquet.`/tmp/delta-demo`");
            }

            var delta = DeltaTable.ForPath("/tmp/delta-demo");
            delta.ToDF().OrderBy(Desc("Id")).Show();

            spark.Range(5, 500).WithColumn("name", Lit("Lucy")).Write().Mode("append").Format("delta")
                .Save("/tmp/delta-demo");

            delta.Update(Expr("id > 500"), new Dictionary<string, Column>
            {
                {"id", Lit(999)}
            });

            delta.Delete(Column("id").EqualTo(999));

            spark.Range(100000, 100100).Write().Format("delta").Mode("append").Save("/tmp/delta-demo");

            delta.History().Show(1000, 10000);

            spark.Read().Format("delta").Option("versionAsOf", 0).Load("/tmp/delta-demo").OrderBy(Desc("Id"))
                .Show();

            spark.Read().Format("delta").Option("timestampAsOf", "2021-10-22 22:03:36")
                .Load("/tmp/delta-demo").OrderBy(Desc("Id")).Show();

            var newData = spark.Range(10).WithColumn("name", Lit("Ed"));

            delta.Alias("target")
                .Merge(newData.Alias("source"), "target.id = source.id")
                .WhenMatched(newData["id"].Mod(2).EqualTo(0)).Update(new Dictionary<string, Column>
                {
                    {"name", newData["name"]}
                })
                .WhenMatched(newData["id"].Mod(2).EqualTo(1)).Delete()
                .WhenNotMatched().InsertAll()
                .Execute();

            delta.ToDF().OrderBy("id").Show(1000, 10000);

            delta.Vacuum(1F);
        }
    }
}