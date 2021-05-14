open Microsoft.Spark.Sql

[<EntryPoint>]
let main argv =
    let BasicDfExample (spark:SparkSession) =
        let dataFrame = spark.Read().Json("/Users/ed/spark-2.4.6-bin-without-hadoop/examples/src/main/resources/people.json")
        dataFrame.Show()
        
        dataFrame.PrintSchema()
        
        dataFrame.Select("name").Show()
        
        dataFrame.Select(dataFrame.["name"], (dataFrame.["age"] + 1)).Show()
        dataFrame.Select(dataFrame.["name"], (dataFrame.["age"].Plus(1))).Show()
        
        dataFrame.Filter(dataFrame.["age"].Gt(21)).Show()
        
        dataFrame.GroupBy(dataFrame.["age"]).Count().Show()
        
        dataFrame.CreateOrReplaceTempView("people")
        let sqlDataFrame = spark.Sql("SELECT * FROM people")
        
        sqlDataFrame.Show()
        
        dataFrame.CreateGlobalTempView("people")
        spark.Sql("SELECT * FROM global_temp.people").Show()
        spark.NewSession().Sql("SELECT * FROM global_temp.people").Show()

    let spark = SparkSession.Builder().GetOrCreate()
    BasicDfExample spark
        
    0
