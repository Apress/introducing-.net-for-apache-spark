using System;
using Microsoft.Spark.ML.Feature;

namespace Listing7_6
{
    class Program
    {
        static void Main(string[] args)
        {
            var tokenizer = new Tokenizer();
            Console.WriteLine(tokenizer.Uid());

            tokenizer = new Tokenizer("a unique identifier");
            Console.WriteLine(tokenizer.Uid());

        }
    }
}