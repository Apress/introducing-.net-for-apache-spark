using System;
using System.Diagnostics;
using Microsoft.Spark.ML.Feature;

namespace Listing7_5
{
    class Program
    {
        static void Main(string[] args)
        {
            var bucketizer = new Bucketizer();
            bucketizer.SetInputCol("input_column");
            bucketizer.Save("/tmp/bucketizer");

            bucketizer.SetInputCol("something_else");

            var loaded = Bucketizer.Load("/tmp/bucketizer");
            Console.WriteLine(bucketizer.GetInputCol());
            Console.WriteLine(loaded.GetInputCol());
        }
    }
}