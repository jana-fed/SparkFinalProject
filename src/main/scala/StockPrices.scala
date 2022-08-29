import Util.{getSpark, myRound, readDataWithView}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.functions.{col, concat_ws, desc, expr, lit, mean, round, stddev_pop, stddev_samp, var_pop, var_samp}


object StockPrices extends App {

  println("Scala Spark Final project")
  val spark = getSpark("Sparky")

  val filePath = "src/resources/stock_prices_.csv"
  //Reading data
  val df = readDataWithView(spark, filePath)
  df.show(3)

  //Calculating Daily Return
  val newDf = df.withColumn("Daily Return",round(expr("((close - open)/open)*100"),2))
  newDf.show()


  //Calculating average daily return
  val meanDf = newDf.groupBy(col("date")).mean("Daily Return")
    .withColumnRenamed("avg(Daily Return)", "avgDailyReturn")
  meanDf.show()


  //Creating DF with every stock daily return
  val aaplDf = newDf.select("date", "Daily Return")
    .where("ticker = 'AAPL'")
    .withColumnRenamed("Daily Return", "AAPL")
    .withColumnRenamed("date","date1")
  val googDf = newDf.select("date", "Daily Return")
    .where("ticker = 'GOOG'")
    .withColumnRenamed("Daily Return", "GOOG")
  val blkDf = newDf.select("date", "Daily Return")
    .where("ticker = 'BLK'")
    .withColumnRenamed("Daily Return", "BLK")
  val msftDf = newDf.select("date", "Daily Return")
    .where("ticker = 'MSFT'")
    .withColumnRenamed("Daily Return", "MSFT")
  val tslaDf = newDf.select("date", "Daily Return")
    .where("ticker = 'TSLA'")
    .withColumnRenamed("Daily Return", "TSLA")

  //Creating Dataframe with Date, Average Daily return and return of all stocks on a date
  val finalDf = meanDf.join(aaplDf,meanDf.col("date") === aaplDf.col("date1"))
    .join(googDf,aaplDf.col("date1") === googDf.col("date"))
    .join(blkDf,aaplDf.col("date1") === blkDf.col("date"))
    .join(msftDf,aaplDf.col("date1") === msftDf.col("date"))
    .join(tslaDf,aaplDf.col("date1") === tslaDf.col("date"))

    .select("date1", "avgDailyReturn","AAPL", "GOOG","BLK","MSFT","TSLA")


  finalDf.show()


//Writing Data to parquet and CSV
//    finalDf.write
//      .format("parquet")
//      .mode("append")
//      .save("src/resources/result/avg_daily_return.parquet")
//  finalDf.write
//    .format("csv")
//    .mode("overwrite")
//    .option("header", true)
//    .save("src/resources/result/avg_daily_return.csv")

//Which stock was traded most frequently - as measured by closing price * volume - on average?
  println("Which Stock was traded most frequently?")
  df.withColumn("frequency", col("close") * col("volume"))
    .groupBy("ticker")
    .agg(mean("frequency").as("tradingFrequency"))
    .orderBy(desc("tradingFrequency"))
    .show()
  // Statistics of Daily return
  newDf.select(
    mean("Daily Return"),
    var_pop("Daily Return"),
    var_samp("Daily Return"),
    stddev_pop("Daily Return"),
    stddev_samp("Daily Return"))
    .show()


  //Which stock was the most volatile as measured by annualized standard deviation of daily returns?

  newDf.groupBy("ticker").agg(stddev_pop("Daily Return"),stddev_samp("Daily Return")).show()

}
