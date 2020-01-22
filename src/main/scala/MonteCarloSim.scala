import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark
import org.apache.spark.sql.{Column, SQLContext, SparkSession,Dataset}

import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory
import scala.io.Source._
import scala.util.Random

object MonteCarloSim {

  val logger = LoggerFactory.getLogger(this.getClass)
  val conf = ConfigFactory.load("application.conf")

  logger.info("Configuration for the Spark Job")
  // Alternate spark session
  val spark = SparkSession.builder
    .master("local")
    .appName("myMonteCarloSimulation")
    .getOrCreate()

  import spark.implicits._

  def main(args: Array[String]): Unit = {

    // fetching data from the config file
    val numOfIterations = conf.getInt("NumberOfSimulations")
    val numOfPortfolios = conf.getInt("numOfPortfolios")
    val portfolio1 = conf.getString("Portfolio1").split(",")
    val portfolio2 = conf.getString("Portfolio2").split(",")
    val portfolio3 = conf.getString("Portfolio3").split(",")
    val portfolio4 = conf.getString("Portfolio4").split(",")
    //println(portfolio1(1).drop(1))

    // Executing Simulations for various stocks
    logger.info("Starting Simulations")
    val sim1 = (portfolio1(0),Simulation(numOfIterations,portfolio1(0),portfolio1(1).drop(1).toInt))
    val sim2 = (portfolio2(0),Simulation(numOfIterations,portfolio2(0),portfolio2(1).drop(1).toInt))
    val sim3 = (portfolio3(0),Simulation(numOfIterations,portfolio3(0),portfolio3(1).drop(1).toInt))
    val sim4 = (portfolio4(0),Simulation(numOfIterations,portfolio4(0),portfolio4(1).drop(1).toInt))

    val result = List(sim1,sim2,sim3,sim4)
    val resultDf = result.toDF("Stocks","Profit")
    resultDf.show()
    resultDf.write.format("csv").save(args(0))
  }

  // Simulation function accepting Number of Iterations, Stock Symbol and Investment as parameter and returning Profit value
  def Simulation (iterations: Int, stock: String,invest: Int): Double = {

      // Using window to access elements from previous rows in DataFrame
      val window = Window.orderBy("timestamp")
      //Use derived column laggingCol to find difference between current and previous row
      val laggingCol = lag(col("close"), 1).over(window)


      // making changes to change column
      // taking log1p
      val change = log1p(col("close") / col("LastClose")) //col("close") - col("LastClose")

    logger.info("Starting API call to fetch Data")
      var url = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=" + stock +"&interval=5min&apikey=3RFGU930UK506URL&datatype=csv"
      // Loading the Stocks into DataFrames
      var res = fromURL(url).mkString.stripMargin.lines.toList
      val csvData: Dataset[String] = spark.sparkContext.parallelize(res).toDS()
      // Loading file into Data Frame
      val df = spark.read.option("header", true).option("inferSchema",true).csv(csvData)
        .drop("open")
        .drop("high")
        .drop("low")
        .drop("volume")
        .withColumn("LastClose", laggingCol)
        .withColumn("Change", change)

      df.printSchema()

      // extracting the change column into a list to create a prediction
      val change_col = df.select("Change").rdd.collect().toList.drop(1)
      val originalClose = df.select(col = "close").rdd.collect().toList.drop(1)

      // extracting random variables from list to create prediction
      val random = new Random

      //-> this function is generating simulation for number of iterations
      /*
    CODE:
    def generateRandomCol():Column={
      col("close")* math.exp((change_col(random.nextInt(change_col.length))).toString().drop(1).dropRight(1).toDouble)
    }
    // Formula Used Above:
    New closing value for a day = original closing value * exp(random among the change values)


    This is creating probables values correctly -- Different simulations
    -------------- however unable to add multiple column to the data frames in one GO hence trying with another approach

    CODE:
    val newdf =
    (1 to numOfIterations).foreach(row => {
      df.withColumn("Sim",generateRandomCol)
    })
    //val newdf = df.withColumn("Sim1",simday1)
     */

      //-> Alternate approach: Create 2D lists with these simulated values
      var simulationList = new ListBuffer[List[Double]]()
      (1 to iterations).foreach(i => {
        val sim = originalClose.map(_.toString().drop(1).dropRight(1).toDouble * math.exp((change_col(random.nextInt(change_col.length))).toString().drop(1).dropRight(1).toDouble))
        simulationList += sim
      })

      // Transposing the resulting simulationg matrix to align it with input dataFrame
      val transposedSimulationList = simulationList.toList.transpose

      val newdf = simulationList.transpose.toDF()
      df.show()
      newdf.show()
      println("This is transposed form of list" + transposedSimulationList)

      // Calculating the profit
      def profit(prices: List[Double]): Double = {
        if (prices.isEmpty) 0
        else (0 until prices.length - 1).foldLeft(0.0)((profit, i) => {
          if (prices(i) < prices(i + 1)) profit + prices(i + 1) - prices(i) else profit
        })
      }

      // Calculating the profits for each simulation for one particular stock
      val meanList = transposedSimulationList.map(each_row => each_row.sum / each_row.size)

      val calProfit = profit(meanList)
      // Calculating mean for the profit

      //println("mean list of stocks " + meanList)
      //println("Profit Calculated " + calProfit)

    // calculating the Profit against Investment
     val profitAgainstInvest = df.select("close").first.getDouble(0)/invest * calProfit
    profitAgainstInvest

  }
}



