import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import breeze.plot._
import plotly._, plotly.element._, plotly.layout._, plotly.Plotly._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

case class HealthData (heartDisease: Double, bmi: Double, smoking: Double, alcohol: Double, stroke: Double,
 physicalHealth: Double, mentalHealth: Double, diffWalking: Double, sex: Double, age: Double, race: Double,
 diabetic: Double, activity: Double, generalHealth: Double, sleep: Double, asthma: Double, kidneyDisease: Double,
 skinCancer: Double)

object App {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("NameOfApp").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val data = sc.textFile("src/heart_2020_cleaned.csv")

    val processData = data.flatMap { line =>
      val fields = line.split(",")
      fields match {
        case Array(hd, b, s, a, st, ph, mh, dw, sex, age, race, d, act, gh, sl, ast, kd, sc) =>
          Some(HealthData(
            if (hd == "Yes") 1.0 else 0.0,
            b.toDouble,
            if (s == "Yes") 1.0 else 0.0,
            if (a == "Yes") 1.0 else 0.0,
            if (st == "Yes") 1.0 else 0.0,
            ph.toDouble,
            mh.toDouble,
            if (dw == "Yes") 1.0 else 0.0,
            if (sex == "Male") 1.0 else 0.0,
            age match {
              case "18-24" => 1.0
              case "25-29" => 2.0
              case "30-34" => 3.0
              case "35-39" => 4.0
              case "40-44" => 5.0
              case "45-49" => 6.0
              case "50-54" => 7.0
              case "55-59" => 8.0
              case "60-64" => 9.0
              case "65-69" => 10.0
              case "70-74" => 11.0
              case "75-79" => 12.0
              case "80 or older" => 13.0
              case _ => 0.0
            },
            race match {
              case "White" => 1.0
              case "Black" => 2.0
              case "American Indian/Alaskan Native" => 3.0
              case "Asian" => 4.0
              case "Hispanic" => 5.0
              case "Other" => 6.0
              case _ => 0.0
            },
            d match {
              case "No, borderline diabetes" => 0.0
              case "No" => 0.0
              case "Yes" => 1.0
              case "Yes (during pregnancy)" => 1.0
            },
            if (act == "Yes") 1.0 else 0.0,
            gh match {
              case "Excellent" => 1.0
              case "Very good" => 2.0
              case "Good" => 3.0
              case "Fair" => 4.0
              case "Poor" => 5.0
              case _ => 0.0
            },
            sl.toDouble,
            if (ast == "Yes") 1.0 else 0.0,
            if (kd == "Yes") 1.0 else 0.0,
            if (sc == "Yes") 1.0 else 0.0
          ))
        case _ => None
      }
    }
    val clean_data = processData.map(h => h.productIterator.mkString(", "))
    clean_data.saveAsTextFile("src/output")


    val bmi = processData.map(_.bmi).collect()
    val f1 = Figure()
    val p1 = f1.subplot(0)
    p1 += breeze.plot.hist(bmi, 30)
    p1.title = "BMI Distribution"

    val physicalHealth = processData.map(_.physicalHealth).collect()
    val f2 = Figure()
    val p2 = f2.subplot(0)
    p2 += breeze.plot.hist(physicalHealth, 10)
    p2.title = "Physical Health Distribution"

    val mentalHealth = processData.map(_.mentalHealth).collect()
    val f3 = Figure()
    val p3 = f3.subplot(0)
    p3 += breeze.plot.hist(mentalHealth, 10)
    p3.title = "Mental Health Distribution"

    val sleep = processData.map(_.sleep).collect()
    val f4 = Figure()
    val p4 = f4.subplot(0)
    p4 += breeze.plot.hist(sleep, 20)
    p4.title = "Sleep Distribution"

    val age = processData.map(_.age).collect()
    val f5 = Figure()
    val p5 = f5.subplot(0)
    p5 += breeze.plot.hist(age, 13)
    p5.title = "Age Distribution"

    val race = processData.map(_.race).collect()
    val f6 = Figure()
    val p6 = f6.subplot(0)
    p6 += breeze.plot.hist(race, 6)
    p6.title = "Race Distribution"

    val genHealth = processData.map(_.generalHealth).collect()
    val f7 = Figure()
    val p7 = f7.subplot(0)
    p7 += breeze.plot.hist(genHealth, 5)
    p7.title = "General Health Distribution"

    val yesCount = processData.map(_.heartDisease).sum().toInt
    val noCount = processData.count().toInt - yesCount
    val chart = Seq(
      Bar(
        Seq("Yes"),
        Seq(yesCount)
      ).withMarker(
        Marker().withColor(Color.RGB(214, 120, 47))
      ),
      Bar(
        Seq("No"),
        Seq(noCount)
      ).withMarker(
        Marker().withColor(Color.RGB(56, 111, 194))
      )
    )
    var layout = Layout(title = "Heart Disease Distribution")
    Plotly.plot(traces = chart, layout = layout, path = "src/charts.html")

    val spark = SparkSession.builder()
      .appName("YourAppName")
      .config("spark.master", "local")
      .getOrCreate()
    import spark.implicits._
    val df: DataFrame = processData.toDF()
    val correlations = df.columns.filter(_ != "heartDisease").map { column =>
      val correlation = df.stat.corr("heartDisease", column)
      (column, correlation)
    }
    val chartData = correlations.map { case (column, correlation) =>
      Bar(Seq(column), Seq(correlation))
    }
    layout = Layout(title = "Correlation with Heart Disease")
    Plotly.plot(traces = chartData, layout = layout, path = "src/charts2.html")
    spark.stop()
    sc.stop()
  }
}
