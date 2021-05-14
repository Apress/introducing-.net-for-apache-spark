open Apache.Arrow
open Apache.Arrow.Types
open Microsoft.Spark.Sql
open Microsoft.Spark.Sql.Types

let totalCostOfAllowableItems(records: RecordBatch): RecordBatch =
    
    let nameColumn  = records.Column "Name" :?> StringArray
    let purchaseColumn = records.Column "Purchase" :?> StringArray
    let costColumn = records.Column "Cost" :?> FloatArray
   
    let shouldInclude (purchase) = purchase <> "Drink"
    
    let count() =
        let mutable costs : float32 array  = Array.zeroCreate purchaseColumn.Length
        for index in 0 .. purchaseColumn.Length - 1 do
            costs.SetValue((if shouldInclude (purchaseColumn.GetString(index)) then costColumn.GetValue(index).Value else float32(0)), index)
            
        costs |> Array.sum
    
    let returnLength = if records.Length > 0 then 1 else 0
    
    let schema = Schema.Builder()
                     .Field(
                        Field("Name", StringType.Default, true))
                     .Field(
                        Field("TotalCostOfAllowableExpenses", FloatType.Default, true)
                        )
                        .Build()    
    
    let data: IArrowArray[] = [|
        nameColumn
        (FloatArray.Builder()).Append(count()).Build()        
    |]
    
    new RecordBatch(schema, data, returnLength)

[<EntryPoint>]
let main argv =
   
    let spark = SparkSession.Builder().GetOrCreate();
            
    let dataFrame = spark.Sql("SELECT 'Ed' as Name, 'Sandwich' as Purchase, 4.95 as Cost UNION ALL SELECT 'Sarah', 'Drink', 2.95 UNION ALL SELECT 'Ed', 'Chips', 1.99 UNION ALL SELECT 'Ed', 'Drink', 3.45  UNION ALL SELECT 'Sarah', 'Sandwich', 8.95")
    let dataFrameWithCost = dataFrame.WithColumn("Cost", dataFrame.["Cost"].Cast("Float"))
    
    dataFrameWithCost.Show()
    
    let structType = StructType ([|
        StructField("Name", StringType())
        StructField("TotalCostOfAllowablePurchases", FloatType())
        |])
    
    let categorized = dataFrameWithCost.GroupBy("Name").Apply(structType, totalCostOfAllowableItems)
    categorized.PrintSchema();
    categorized.Show();
    
    0
