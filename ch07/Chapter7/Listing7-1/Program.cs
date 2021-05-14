using System;
using System.Diagnostics.Tracing;
using Microsoft.Spark.ML.Feature;
using Microsoft.Spark.ML.Feature.Param;

namespace Listing7_1
{
    class Program
    {
        static void Main(string[] args)
        {
            var word2Vec = new Word2Vec();
            word2Vec.SetSeed(123);

            Console.WriteLine(word2Vec.ExplainParams());

            Console.WriteLine("~~~~~~");

            var seedParam = new Param(word2Vec, "seed", "Setting the seed to 54321");
            word2Vec.Set(seedParam, 54321L);

            Console.WriteLine(word2Vec.ExplainParams());

            Console.WriteLine("~~~~~~");

            var seed = word2Vec.GetParam("seed");
            word2Vec.Set(seed, 12345L);
            Console.WriteLine(word2Vec.ExplainParams());

            word2Vec.Clear(seed);
            Console.WriteLine(word2Vec.ExplainParams());

        }
    }
}