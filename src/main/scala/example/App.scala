package example
import scala.io._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
// import breeze.plot._
// import plotly._, plotly.element._, plotly.layout._, plotly.Plotly._
// import org.apache.spark.sql.functions._
// import org.apache.spark.sql.{DataFrame, SparkSession}

case class LoanApplicant (id : Double, loanType: Double, gender: Double, ownCar: Double, ownRealty: Double, income: Double, credit: Double, annuity: Double,
 age: Double, employment: Double, registration: Double, education: Double, familyStatus: Double,
 houseType: Double, numChildren: Double, relativePopulation: Double)

object App {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("project")
    val sc = new SparkContext(conf)

    val data = sc.textFile("/user/isathaye/input/application.csv").persist()

    val processData = data.flatMap { line =>
    val fields = line.split(",")
    fields match {
      case Array(
            id, loanType, gender, ownCar, ownRealty, income, credit, annuity, age, employment,
            registration, education, familyStatus, houseType, numChildren, relativePopulation
          ) =>
        Some(LoanApplicant(
            id.toDouble, 
          loanType.toDouble,
          if (gender == "F") 1.0 else 0.0,
          ownCar.toDouble,
          ownRealty.toDouble,
          income.toDouble,
          credit.toDouble,
          annuity.toDouble,
          age.toDouble,
          employment.toDouble,
          registration.toDouble,
          education match {
              case "Secondary / secondary special" => 1.0
              case "Higher education" => 2.0
              case "Incomplete higher" => 3.0
              case "Lower secondary" => 4.0
              case "Academic degree" => 5.0
              case _ => 0.0
          },
          familyStatus match {
            case "Single / not married" => 1.0
            case "Married" => 2.0
            case "Civil marriage" => 3.0
            case "Widow" => 4.0
            case "Separated" => 5.0
            case "Unknown" => 6.0
            case _ => 0.0
          },
          houseType match {
            case "House / apartment" => 1.0
            case "Rented apartment" => 2.0
            case "With parents" => 3.0
            case "Municipal apartment" => 4.0
            case "Office apartment" => 5.0
            case "Co-op apartment" => 6.0
            case _ => 0.0
          },
          numChildren.toDouble,
          relativePopulation.toDouble
        ))
      case _ => None
    }
  }



    val clean_data = processData.map(h => h.productIterator.mkString(", "))
    clean_data.saveAsTextFile("/user/isathaye/output")


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
