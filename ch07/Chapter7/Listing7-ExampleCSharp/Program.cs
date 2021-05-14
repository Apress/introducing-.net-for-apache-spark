using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Xml;
using Microsoft.Spark.ML.Feature;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using static Microsoft.Spark.Sql.Functions;

namespace Listing7_1
{
    internal class Program
    {
        private static Func<Column, Column> PrintVector = Udf<Row, bool>(row =>
        {
            Console.WriteLine(row.Schema);
            var start = row.Values[0];
            var size = row.Values[1];

            var indices = (ArrayList)row.Values[2];
            var values = (ArrayList)row.Values[3];
            Console.Write($"a: {start} to: {size}");

            for (var i = 0; i < indices.Count; i++)
                Console.WriteLine($"{i}, {indices[i]}, {values[i]}");

            return true;
        });

        private static readonly Func<Column, Column, Column, Column, Column> udfCosineSimilarity =
            Udf<Row, Row, double, double, double>(
                (vectorA, vectorB, normA, normB) =>
                {
                    var indicesA = (ArrayList)vectorA.Values[2];
                    var valuesA = (ArrayList)vectorA.Values[3];

                    var indicesB = (ArrayList)vectorB.Values[2];
                    var valuesB = (ArrayList)vectorB.Values[3];

                    var dotProduct = 0.0;

                    for (var i = 0; i < indicesA.Count; i++)
                    {
                        var valA = (double)valuesA[i];

                        var indexB = findIndex(indicesB, 0, (int)indicesA[i]);

                        double valB = 0;
                        if (indexB != -1)
                            valB = (double)valuesB[indexB];
                        else
                            valB = 0;

                        dotProduct += valA * valB;
                    }

                    var divisor = normA * normB;

                    return divisor == 0 ? 0 : dotProduct / divisor;
                });

        private static readonly Func<Column, Column> udfCalcNorm = Udf<Row, double>(row =>
            {
                var values = (ArrayList)row.Values[3];
                var norm = 0.0;

                foreach (var value in values)
                {
                    var d = (double)value;
                    norm += d * d;
                }

                return Math.Sqrt(norm);
            }
        );

        private static List<GenericRow> GetDocuments(string path)
        {
            var documents = new List<GenericRow>();

            foreach (var file in new DirectoryInfo(path).EnumerateFiles("*.xml",
                SearchOption.AllDirectories))
            {
                var doc = new XmlDocument();

                doc.Load(file.FullName);

                var playTitle = "";
                var title = doc.SelectSingleNode("//title");

                playTitle = title != null
                    ? title.InnerText
                    : doc.SelectSingleNode("//personae[@playtitle]").Attributes["playtitle"].Value;

                var play = doc.SelectSingleNode("//play");

                if (play != null)
                {
                    documents.Add(new GenericRow(new[] { playTitle, play.InnerText }));
                }
                else
                {
                    var poem = doc.SelectSingleNode("//poem");
                    documents.Add(new GenericRow(new[] { playTitle, poem.InnerText }));
                }
            }

            return documents;
        }

        private static void Main(string[] args)
        {
            var spark = SparkSession
                .Builder()
                .AppName("TF-IDF Application")
                .GetOrCreate();

            var documentPath = args[0];
            var search = args[1];

            var documentData = GetDocuments(documentPath);

            var documents = spark.CreateDataFrame(documentData, new StructType(
                new List<StructField>
                {
                    new StructField("title", new StringType()),
                    new StructField("content", new StringType())
                }));

            var tokenizer = new Tokenizer()
                .SetInputCol("content")
                .SetOutputCol("words");

            var hashingTF = new HashingTF()
                .SetInputCol("words")
                .SetOutputCol("rawFeatures")
                .SetNumFeatures(1000000);

            var idf = new IDF()
                .SetInputCol("rawFeatures")
                .SetOutputCol("features");

            var tokenizedDocuments = tokenizer.Transform(documents);
            var featurizedDocuments = hashingTF.Transform(tokenizedDocuments);

            var idfModel = idf.Fit(featurizedDocuments);

            var transformedDocuments =
                idfModel.Transform(featurizedDocuments).Select("title", "features");
            var normalizedDocuments = transformedDocuments.Select(Col("features"),
                udfCalcNorm(transformedDocuments["features"]).Alias("norm"), Col("title"));

            var searchTerm = spark.CreateDataFrame(
                new List<GenericRow> { new GenericRow(new[] { search }) },
                new StructType(new[] { new StructField("content", new StringType()) }));

            var tokenizedSearchTerm = tokenizer.Transform(searchTerm);

            var featurizedSearchTerm = hashingTF.Transform(tokenizedSearchTerm);

            var normalizedSearchTerm = idfModel
                .Transform(featurizedSearchTerm)
                .WithColumnRenamed("features", "searchTermFeatures")
                .WithColumn("searchTermNorm", udfCalcNorm(Column("searchTermFeatures")));

            var results = normalizedDocuments.CrossJoin(normalizedSearchTerm);

            results
                .WithColumn("similarity",
                    udfCosineSimilarity(Column("features"), Column("searchTermFeatures"),
                        Col("norm"), Col("searchTermNorm")))
                .OrderBy(Desc("similarity")).Select("title", "similarity")
                .Show(10000, 100);
        }

        private static int findIndex(ArrayList list, int currentIndex, int wantedValue)
        {
            for (var i = currentIndex; i < list.Count; i++)
                if ((int)list[i] == wantedValue)
                    return i;

            return -1;
        }
    }
}