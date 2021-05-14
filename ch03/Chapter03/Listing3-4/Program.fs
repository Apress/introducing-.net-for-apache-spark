open Microsoft.Spark.Sql
open System

[<EntryPoint>]
let main argv =
    let spark = SparkSession
                    .Builder()
                    .AppName("DemoApp")
                    .GetOrCreate()
                    
    let dataFrame = spark.Sql("select id, rand() as random_number from range(1000)")
    
    dataFrame
                .Write()
                .Format("csv")
                .Option("header", true)
                .Option("sep", "|")
                .Mode("overwrite")
                .Save(argv.[1]);

    dataFrame.Collect()
                    |> Seq.map(fun row -> row.Get(0) :?> int)
                    |> Seq.filter(fun id -> id % 2 = 0)
                    |> Seq.iter(fun i -> printfn "row: %d" i)
                                            
    0
