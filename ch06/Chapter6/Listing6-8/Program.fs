open Microsoft.Spark.Sql
open Razorvine.Pickle

[<EntryPoint>]
let main argv =
    
    let spark = SparkSession.Builder().GetOrCreate()
    spark.Sql("CREATE DATABASE InputData")
    spark.Catalog.SetCurrentDatabase "InputData"
    spark.Catalog.CreateTable("id_list", "./ID.parquet")

    let getTableDefinition = 
        let getColumn(column:Row) =
           sprintf "%s\t%s" (column.[0].ToString()) (column.[2].ToString())
            
        let getColumns(dbName:string, tableName:string) =
            spark.Catalog.ListColumns(dbName, tableName)
                                   |> fun c -> c.Collect()
                                   |> Seq.map(fun column -> getColumn(column))
                                   |> String.concat "\n"
                                     
        let getTable (table:Row) =
            let databaseName = table.[1].ToString()
            let tableName = table.[0].ToString()
            
            let tableHeader = sprintf "Database: %s, Table: %s" databaseName tableName
            let columnDefinition = getColumns(databaseName, tableName)
                                   
            sprintf "%s\n%s" tableHeader columnDefinition
        
        let tableDefinition =
            spark.Catalog.ListTables "InputData"
            |> fun t -> t.Collect()
            |> Seq.map (fun table -> getTable(table))
            
        tableDefinition

    PrettyPrint.print getTableDefinition
     
    0
