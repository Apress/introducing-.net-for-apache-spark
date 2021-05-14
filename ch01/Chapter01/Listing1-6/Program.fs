open Microsoft.Spark.Sql
open System

[<EntryPoint>]
let main argv =
    let spark = SparkSession.Builder().GetOrCreate()
    spark.Range(100L).WithColumn("Name", Functions.Lit("Ed"))
    |> fun d -> d.Union(spark.Range(100L).WithColumn("Name", Functions.Lit("Bert")))
    |> fun d -> d.Union(spark.Range(100L).WithColumn("Name", Functions.Lit("Lillian")))
    |> fun d -> d.GroupBy(Functions.Col("Name")).Count()
    |> fun d-> d.Show()
    
    0 
