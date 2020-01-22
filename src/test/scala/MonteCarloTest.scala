import MonteCarloSim.{Simulation, conf}
import com.typesafe.config.ConfigFactory
import org.scalatest.FunSuite

import scala.xml.XML


class MonteCarloTest extends FunSuite {
  val conf = ConfigFactory.load("application.conf")

  test("Able to Load Configuration") {

    val iterations = conf.getString("NumberOfSimulations")
    val PATH_OUTPUT2 = conf.getString("OutputPath")

    assert(!PATH_OUTPUT2.isEmpty)
    assert(!iterations.isEmpty)
  }

  test("Test for API to fetch Data ") {
    // Simulation funtion calls the api and loads the data to the Dataframe, without which the function would fail
    val portfolio1 = conf.getString("Portfolio1").split(",")
    val sim1 = (portfolio1(0),Simulation(10,portfolio1(0),portfolio1(1).drop(1).toInt))

    assert(sim1 != null)
  }


  test("Profit Calculated correctness") {
    // using Logic from Calculating Profit
    var prices: List[Int] = List(5, 4, 1, 2, 3)
    val profit = if (prices.isEmpty) 0
    else (0 until prices.length - 1).foldLeft(0)((profit, i) => {
      if (prices(i) < prices(i + 1)) profit + prices(i + 1) - prices(i) else profit
    })

    assert(profit == 2)
  }

  test ("Each stock has an associated investment")
  {
    // Some Money is invested in stock
    val portfolio1 = conf.getString("Portfolio1").split(",")
    assert(!portfolio1(1).isEmpty)
  }
  test ("Correct Stock Symbol is used"){
    val symbols = "AAL,AAP,AAPL,ABBV,ABC,ABMD,ABT,ACN,ADBE,ADI,ADM,ADP,ADS,ADSK,AEE,AEP,AES,AFL,AGN,AIG,AIV,AIZ,AJG,AKAM,ALB,ALGN,ALK,ALL,ALLE,ALXN,AMAT,AMCR,AMD,AME,AMG,AMGN,AMP,AMT,AMZN,ANET,ANSS,ANTM,AON,AOS,APA,APD,APH,APTV,ARE,ARNC,ATO,ATVI,AVB,AVGO,AVY,AWK,AXP,AZO,BA,BAC,BAX,BBT,BBY,BDX,BEN,BF.B,BIIB,BK,BKNG,BKR,BLK,BLL,BMY,BR,BRK.B,BSX,BWA,BXP,C,CAG,CAH,CAT,CB,CBOE,CBRE,CBS,CCI,CCL,CDNS,CDW,CE,CERN,CF,CFG,CHD,CHRW,CHTR,CI,CINF,CL,CLX,CMA,CMCSA,CME,CMG,CMI,CMS,CNC,CNP,COF,COG,COO,COP,COST,COTY,CPB,CPRI,CPRT,CRM,CSCO,CSX,CTAS,CTL,CTSH,CTVA,CTXS,CVS,CVX,CXO,D,DAL,DD,DE,DFS,DG,DGX,DHI,DHR,DIS,DISCA,DISCK,DISH,DLR,DLTR,DOV,DOW,DRE,DRI,DTE,DUK,DVA,DVN,DXC,EA,EBAY"
    val symbolList = symbols.split(",")
    val portfolio1 = conf.getString("Portfolio1").split(",")

    assert(symbolList.contains(portfolio1(0)))
  }

}

