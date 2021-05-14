using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.Spark.Extensions.Delta.Tables;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using static Microsoft.Spark.Sql.Functions;

namespace Listing8_1
{
    public static class OfgemExpensesEntity
    {
        private static readonly StructType _expectedSchema = new StructType(new List<StructField>
        {
            new StructField("Date", new DateType()),
            new StructField("Expense_Type", new StringType()),
            new StructField("Expense_Area", new StringType()),
            new StructField("Supplier", new StringType()),
            new StructField("Reference", new StringType()),
            new StructField("Amount", new FloatType())
        });

        private static readonly List<string> ColumnsToKeep = new List<string>
        {
            "Date", "Expense Type", "Expense Area", "Supplier", "Reference", "Amount"
        };

        public static DataFrame ReadFromSource(SparkSession spark, string path)
        {
            var dataFrame = spark.Read().Format("csv").Options(
                new Dictionary<string, string>
                {
                    {"inferSchema", "false"},
                    {"header", "false"},
                    {"encoding", "ISO-8859-1"},
                    {"locale", "en-GB"},
                    {"quote", "\""},
                    {"ignoreLeadingWhiteSpace", "true"},
                    {"ignoreTrailingWhiteSpace", "true"},
                    {"dateFormat", "M y"}
                }
            ).Load(path);

            //Add an index column with an increasing id for each row
            var dataFrameWithId = dataFrame.WithColumn("index", MonotonicallyIncreasingId());

            //Pull out the column names
            var header = dataFrameWithId.Filter(Col("index") == 1).Collect();

            //filter out the header rows
            var filtered = dataFrameWithId.Filter(Col("index") > 1).Drop("index");

            var columnNames = new List<string>();
            var headerRow = header.First();

            for (var i = 0; i < headerRow.Values.Length; i++)
                if (headerRow[i] == null || !ColumnsToKeep.Contains(headerRow[i]))
                {
                    Console.WriteLine($"DROPPING: _c{i}");
                    filtered = filtered.Drop($"_c{i}");
                }
                else
                {
                    columnNames.Add((headerRow[i] as string).Replace(" ", "_"));
                }

            var output = filtered.ToDF(columnNames.ToArray());

            output = output.Filter(Col("Reference").IsNotNull() & Col("Supplier").IsNotNull());

            output = output.WithColumn("Amount", RegexpReplace(Col("Amount"), "[£,]", ""));
            output = output.WithColumn("Amount", Col("Amount").Cast("float"));

            output = output.WithColumn("OriginalDate", Col("Date"));
            output = output.WithColumn("Date", ToDate(Col("Date"), "MMMM yyyy"));

            if (output.Filter(Col("Date").IsNull()).Count() == output.Count())
            {
                Console.WriteLine("Trying alternate date format...");
                output = output.WithColumn("Date", ToDate(Col("OriginalDate"), "MMM-yy"));
            }

            output = output.Drop("OriginalDate");

            output.Show(1000, 10000);

            return output;
        }

        public static void WriteToStructured(DataFrame data, string path)
        {
            data.Write().Mode("overwrite").Format("parquet").Save(path);
        }

        public static void WriteToCurated(DataFrame data, string path)
        {
            if (ValidateEntity(data))
            {
                data.WithColumn("year", Year(Col("Date")))
                    .WithColumn("month", Month(Col("Date")))
                    .Write()
                    .PartitionBy("year", "month")
                    .Mode("overwrite")
                    .Parquet(path);
            }
            else
            {
                Console.WriteLine("Validation Failed, writing failed file.");
                data.Write().Mode("overwrite").Parquet($"{path}-failed");
            }
        }

        public static void WriteToPublish(SparkSession spark, string rootPath, string publishPath)
        {
            var data = spark.Read().Parquet(rootPath);
            var suppliers = data.Select(Col("Supplier")).Distinct()
                .WithColumn("supplier_hash", Hash(Col("Supplier")));

            var supplierPublishPath = $"{publishPath}-suppliers";

            if (!Directory.Exists(supplierPublishPath))
            {
                suppliers.Write().Format("delta").Save(supplierPublishPath);
            }
            else
            {
                var existingSuppliers = spark.Read().Format("delta").Load(supplierPublishPath);
                var newSuppliers = suppliers.Join(existingSuppliers,
                    existingSuppliers["Supplier"] == suppliers["Supplier"], "left_anti");
                newSuppliers.Write().Mode(SaveMode.Append).Format("delta")
                    .Save(supplierPublishPath);
            }

            var expenseTypePublishPath = $"{publishPath}-expense-type";

            var expenseType = data.Select(Col("Expense_Type")).Distinct()
                .WithColumn("expense_type_hash", Hash(Col("Expense_Type")));

            if (!Directory.Exists(expenseTypePublishPath))
            {
                expenseType.Write().Format("delta").Save(expenseTypePublishPath);
            }
            else
            {
                var existingExpenseType =
                    spark.Read().Format("delta").Load(expenseTypePublishPath);
                var newExpenseType = expenseType.Join(existingExpenseType,
                    existingExpenseType["Expense_Type"] == expenseType["Expense_Type"],
                    "left_anti");
                newExpenseType.Write().Mode(SaveMode.Append).Format("delta")
                    .Save(expenseTypePublishPath);
            }

            data = data.WithColumn("Expense_Type", Hash(Col("Expense_Type")))
                .WithColumn("Supplier", Hash(Col("Supplier")));

            if (!Directory.Exists(publishPath))
            {
                data.Write().Format("delta").Save(publishPath);
            }
            else
            {
                var target = DeltaTable.ForPath(publishPath).Alias("target");
                target.Merge(
                        data.Alias("source"),
                        "source.Date = target.Date AND source.Expense_Type = target.Expense_Type AND source.Expense_Area = target.Expense_Area AND source.Supplier = target.supplier AND source.Reference = target.Reference"
                    ).WhenMatched("source.Amount != target.Amount")
                    .Update(new Dictionary<string, Column> { { "Amount", data["Amount"] } }
                    ).WhenNotMatched()
                    .InsertAll()
                    .Execute();
            }
        }

        private static bool ValidateEntity(DataFrame data)
        {
            var ret = true;

            if (data.Schema().Json != _expectedSchema.Json)
            {
                Console.WriteLine("Expected Schema Does NOT Match");
                Console.WriteLine("Actual Schema: " + data.Schema().SimpleString);
                Console.WriteLine("Expected Schema: " + _expectedSchema.SimpleString);
                ret = false;
            }

            if (data.Filter(Col("Date").IsNotNull()).Count() == 0)
            {
                Console.WriteLine("Date Parsing resulted in all NULL's");
                ret = false;
            }

            if (data.Count() == 0)
            {
                Console.WriteLine("DataFrame is empty");
                ret = false;
            }

            var amountBySuppliers = data.GroupBy(Col("Supplier")).Sum("Amount")
                .Filter(Col("Sum(Amount)") < 25000);

            if (amountBySuppliers.Count() > 0)
            {
                Console.WriteLine("Amounts should only ever be over 25k");
                amountBySuppliers.Show();
                ret = false;
            }

            return ret;
        }
    }
}


/*
 * overall process - read each file and write to staging where we do a merge into the final table (maybe use dimensions and facts)
 *
 * 
 * talking points:
 *         writing the file into staging - is it overwrite? then we can just re run in every file every time
 *                                         - append - can only do each file once
 *                                         - merge - can redo
 *
 *
 * 
 */