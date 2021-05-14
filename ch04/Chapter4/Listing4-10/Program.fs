open Microsoft.Data.Analysis
open Microsoft.Spark.Sql


[<EntryPoint>]
let main argv =
    
    let spark = SparkSession.Builder().GetOrCreate();

    let dataFrame = spark.Sql("SELECT ID FROM range(1000)");
            
    let add100 = DataFrameFunctions.VectorUdf<Int64DataFrameColumn, Int64DataFrameColumn, Int64DataFrameColumn>( fun first second -> first.Add(second))
    
    dataFrame.Select(add100.Invoke(dataFrame.["ID"], dataFrame.["ID"])).Show()
    
    0 
