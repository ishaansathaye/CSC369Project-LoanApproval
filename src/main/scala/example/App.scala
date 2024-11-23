package example
import scala.io._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
// import breeze.plot._
// import plotly._, plotly.element._, plotly.layout._, plotly.Plotly._
// import org.apache.spark.sql.functions._
// import org.apache.spark.sql.{DataFrame, SparkSession}

case class LoanApplicant(
  SK_ID_CURR: Double,
  TARGET: Double,
  NAME_CONTRACT_TYPE: Double,
  FLAG_OWN_CAR: Double,
  FLAG_OWN_REALTY: Double,
  AMT_INCOME_TOTAL: Double,
  AMT_CREDIT: Double,
  AMT_ANNUITY: Double,
  AMT_GOODS_PRICE: Double,
  REGION_POPULATION_RELATIVE: Double,
  CNT_FAM_MEMBERS: Double,
  REGION_RATING_CLIENT: Double,
  EXT_SOURCE_2: Double,
  EXT_SOURCE_3: Double,
  YEARS_BEGINEXPLUATATION_AVG: Double,
  OBS_60_CNT_SOCIAL_CIRCLE: Double,
  DEF_60_CNT_SOCIAL_CIRCLE: Double,
  AMT_REQ_CREDIT_BUREAU_YEAR: Double,
  AGE: Double,
  YEARS_EMPLOYED: Double,
  YEARS_REGISTERED: Double,
  YEARS_ID_PUBLISH: Double
)



object App {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("project").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // val data = sc.textFile("/user/isathaye/input/application.csv").persist()
    val data = sc.textFile("./src/application.csv").persist()

    val header = data.first()
    val rows = data.filter(_ != header)

    val processData = rows.flatMap{line =>
  val fields = line.split(",")
  fields match {
    case Array(
          id, target, loanType, ownCar, ownRealty, income, credit, annuity, goodsPrice, population, famMembers, regionRating, source2, source3, yearsExpl, obs60, def60, creditCheckYear, age, yearsEmployed, yearsReg, yearsPub) =>


      Some(LoanApplicant(
        id.toDouble,
        target.toDouble,
        if (loanType == "Cash loans") 1.0 else 0.0,
        if (ownCar == "Y") 1.0 else 0.0,
        if (ownRealty == "Y") 1.0 else 0.0,
        income.toDouble,
        credit.toDouble,
        annuity.toDouble,
        goodsPrice.toDouble,
        population.toDouble,
        famMembers.toDouble,
        regionRating.toDouble,
        source2.toDouble,
        source3.toDouble,
        yearsExpl.toDouble,
        obs60.toDouble,
        def60.toDouble,
        creditCheckYear.toDouble,
        age.toDouble,
        yearsEmployed.toDouble,
        yearsReg.toDouble,
        yearsPub.toDouble
      ))
    case _ => None
  }
}  

    val clean_data = processData.map(h => h.productIterator.mkString(", "))
    // clean_data.saveAsTextFile("/user/isathaye/output")
    clean_data.saveAsTextFile("./src/output")


    // val bmi = processData.map(_.bmi).collect()
    // val f1 = Figure()
    // val p1 = f1.subplot(0)
    // p1 += breeze.plot.hist(bmi, 30)
    // p1.title = "BMI Distribution"

    // val physicalHealth = processData.map(_.physicalHealth).collect()
    // val f2 = Figure()
    // val p2 = f2.subplot(0)
    // p2 += breeze.plot.hist(physicalHealth, 10)
    // p2.title = "Physical Health Distribution"

    // val mentalHealth = processData.map(_.mentalHealth).collect()
    // val f3 = Figure()
    // val p3 = f3.subplot(0)
    // p3 += breeze.plot.hist(mentalHealth, 10)
    // p3.title = "Mental Health Distribution"

    // val sleep = processData.map(_.sleep).collect()
    // val f4 = Figure()
    // val p4 = f4.subplot(0)
    // p4 += breeze.plot.hist(sleep, 20)
    // p4.title = "Sleep Distribution"

    // val age = processData.map(_.age).collect()
    // val f5 = Figure()
    // val p5 = f5.subplot(0)
    // p5 += breeze.plot.hist(age, 13)
    // p5.title = "Age Distribution"

    // val race = processData.map(_.race).collect()
    // val f6 = Figure()
    // val p6 = f6.subplot(0)
    // p6 += breeze.plot.hist(race, 6)
    // p6.title = "Race Distribution"

    // val genHealth = processData.map(_.generalHealth).collect()
    // val f7 = Figure()
    // val p7 = f7.subplot(0)
    // p7 += breeze.plot.hist(genHealth, 5)
    // p7.title = "General Health Distribution"

    // val yesCount = processData.map(_.heartDisease).sum().toInt
    // val noCount = processData.count().toInt - yesCount
    // val chart = Seq(
    //   Bar(
    //     Seq("Yes"),
    //     Seq(yesCount)
    //   ).withMarker(
    //     Marker().withColor(Color.RGB(214, 120, 47))
    //   ),
    //   Bar(
    //     Seq("No"),
    //     Seq(noCount)
    //   ).withMarker(
    //     Marker().withColor(Color.RGB(56, 111, 194))
    //   )
    // )
    // var layout = Layout(title = "Heart Disease Distribution")
    // Plotly.plot(traces = chart, layout = layout, path = "src/charts.html")

    // val spark = SparkSession.builder()
    //   .appName("YourAppName")
    //   .config("spark.master", "local")
    //   .getOrCreate()
    // import spark.implicits._
    // val df: DataFrame = processData.toDF()
    // val correlations = df.columns.filter(_ != "heartDisease").map { column =>
    //   val correlation = df.stat.corr("heartDisease", column)
    //   (column, correlation)
    // }
    // val chartData = correlations.map { case (column, correlation) =>
    //   Bar(Seq(column), Seq(correlation))
    // }
    // layout = Layout(title = "Correlation with Heart Disease")
    // Plotly.plot(traces = chartData, layout = layout, path = "src/charts2.html")
    // spark.stop()
    
    sc.stop()
  }
}
