open Microsoft.Spark.Sql
open System   

[<EntryPoint>]
let main argv =
    
    let writeResults (x:DataFrame) =
        x.Write().Mode("overwrite").Parquet("output.parquet")
        printfn "Wrote: %u rows" (x.Count())
    
    let spark = SparkSession.Builder().GetOrCreate()
    spark.Read().Parquet("1.parquet")
    |> fun p -> p.Filter(Functions.Col("Event_Type").EqualTo(Functions.Lit(999)))
    |> fun filtered -> writeResults filtered
    
    0 // return an integer exit code
