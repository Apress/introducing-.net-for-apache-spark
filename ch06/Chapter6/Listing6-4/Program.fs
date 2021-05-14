open Microsoft.Spark.Sql
open System

[<EntryPoint>]
let main argv =
    let spark = SparkSession.Builder().Config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true").GetOrCreate()

    spark.CreateDataFrame([|10;11;12;13;14;15|])
        |> fun dataFrame -> dataFrame.WithColumnRenamed("_1", "ID")
        |> fun dataFrame -> dataFrame.Write().SaveAsTable("saved_table")

    spark.Sql("select * from saved_table").Show()
    0
