open Microsoft.Spark.Extensions.Delta.Tables
open Microsoft.Spark.Sql
open System.Collections.Generic

[<EntryPoint>]
let main argv =
    let spark = SparkSession.Builder()
                |> fun builder -> builder.Config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                |> fun builder -> builder.GetOrCreate()
        
    let delta = match DeltaTable.IsDeltaTable("parquet.`/tmp/delta-demo`") with
                    | false -> spark.Range(1000L)
                                    |> fun dataframe -> dataframe.WithColumn("name", Functions.Lit("Sammy"))
                                    |> fun dataframe -> dataframe.Write()
                                    |> fun writer -> writer.Mode("overwrite").Parquet("/tmp/delta-demo")
                               DeltaTable.ConvertToDelta(spark, "parquet.`/tmp/delta-demo`")
                    | _ -> DeltaTable.ForPath("/tmp/delta-demo")

    spark.Range(5L, 500L)
        |> fun dataframe -> dataframe.WithColumn("name", Functions.Lit("Lucy"))
        |> fun dataframe -> dataframe.Write()
        |> fun writer -> writer.Mode("append").Format("delta").Save("/tmp/delta-demo")
        
    delta.Update(Functions.Expr("id > 500"), Dictionary<string, Column>(dict [("id", Functions.Lit(999))]))
                     
    delta.Delete(Functions.Col("id").EqualTo(999))
    
    delta.History()
        |> fun dataframe -> dataframe.Show()
    
    spark.Read()
        |> fun reader -> reader.Format("delta")
        |> fun reader -> reader.Option("versionAsOf", 0L)
        |> fun reader -> reader.Load("/tmp/delta-demo")
        |> fun dataframe -> dataframe.OrderBy(Functions.Desc("id"))
        |> fun ordered -> ordered.Show()
        
    
    spark.Read()
        |> fun reader -> reader.Format("delta")
        |> fun reader -> reader.Option("timestampAsOf", "2022-01-01")
        |> fun reader -> reader.Load("/tmp/delta-demo")
        |> fun dataframe -> dataframe.OrderBy(Functions.Desc("id"))
        |> fun ordered -> ordered.Show()

    let newData = spark.Range(10L)
                     |> fun dataframe -> dataframe.WithColumn("name", Functions.Lit("Ed"))
                     |> fun newData -> newData.Alias("source")
      
    
    delta.Alias("target")
      |> fun target -> target.Merge(newData, "source.id = target.id")
      |> fun merge -> merge.WhenMatched(newData.["id"].Mod(2).EqualTo(0))
      |> fun evens -> evens.Update(Dictionary<string, Column>(dict [("name", newData.["name"])]))
      |> fun merge -> merge.WhenMatched(newData.["id"].Mod(2).EqualTo(0))
      |> fun odds -> odds.Delete()
      |> fun merge -> merge.WhenNotMatched()
      |> fun inserts -> inserts.InsertAll()
      |> fun merge -> merge.Execute()
      
    delta.ToDF()
        |> fun dataframe -> dataframe.OrderBy("id")
        |> fun ordered -> ordered.Show()
    
    delta.Vacuum(1.0)

    0
