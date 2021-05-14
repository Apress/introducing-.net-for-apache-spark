using System;
using Microsoft.Spark.Sql;

namespace TransformingData_SQL
{
    class Program
    {
        static void Main(string[] args)
        {
            var spark = SparkSession
                .Builder()
                .GetOrCreate();

            var data = spark.Sql(@"
                WITH users
                AS (
                    SELECT ID, 'Ed' as Name FROM Range(100)
                    UNION ALL 
                    SELECT ID, 'Bert' as Name FROM Range(100)
                    UNION ALL 
                    SELECT ID, 'Lillian' as Name FROM Range(100)
                ) SELECT Name, COUNT(*) FROM users GROUP BY Name
            ");

            data.Show();
        }
    }
}