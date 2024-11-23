package example
import scala.io._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
// import breeze.plot._
// import plotly._, plotly.element._, plotly.layout._, plotly.Plotly._
// import org.apache.spark.sql.functions._
// import org.apache.spark.sql.{DataFrame, SparkSession}

case class LoanApplicant (
  SK_ID_CURR: Double,
  TARGET: Double,
  NAME_CONTRACT_TYPE: Double,
  CODE_GENDER: Double,
  FLAG_OWN_CAR: Double,
  FLAG_OWN_REALTY: Double,
  CNT_CHILDREN: Double,
  AMT_INCOME_TOTAL: Double,
  AMT_CREDIT: Double,
  AMT_ANNUITY: Double,
  AMT_GOODS_PRICE: Double,
  REGION_POPULATION_RELATIVE: Double,
  FLAG_EMP_PHONE: Double,
  FLAG_WORK_PHONE: Double,
  FLAG_CONT_MOBILE: Double,
  FLAG_PHONE: Double,
  FLAG_EMAIL: Double,
  CNT_FAM_MEMBERS: Double,
  REGION_RATING_CLIENT: Double,
  REGION_RATING_CLIENT_W_CITY: Double,
  REG_REGION_NOT_LIVE_REGION: Double,
  REG_REGION_NOT_WORK_REGION: Double,
  LIVE_REGION_NOT_WORK_REGION: Double,
  REG_CITY_NOT_LIVE_CITY: Double,
  REG_CITY_NOT_WORK_CITY: Double,
  LIVE_CITY_NOT_WORK_CITY: Double,
  EXT_SOURCE_2: Double,
  EXT_SOURCE_3: Double,
  YEARS_BEGINEXPLUATATION_AVG: Double,
  FLOORSMAX_AVG: Double,
  OBS_30_CNT_SOCIAL_CIRCLE: Double,
  DEF_30_CNT_SOCIAL_CIRCLE: Double,
  OBS_60_CNT_SOCIAL_CIRCLE: Double,
  DEF_60_CNT_SOCIAL_CIRCLE: Double,
  DAYS_LAST_PHONE_CHANGE: Double,
  FLAG_DOCUMENT_2: Double,
  FLAG_DOCUMENT_3: Double,
  FLAG_DOCUMENT_4: Double,
  FLAG_DOCUMENT_5: Double,
  FLAG_DOCUMENT_6: Double,
  FLAG_DOCUMENT_7: Double,
  FLAG_DOCUMENT_8: Double,
  FLAG_DOCUMENT_9: Double,
  FLAG_DOCUMENT_10: Double,
  FLAG_DOCUMENT_11: Double,
  FLAG_DOCUMENT_12: Double,
  FLAG_DOCUMENT_13: Double,
  FLAG_DOCUMENT_14: Double,
  FLAG_DOCUMENT_15: Double,
  FLAG_DOCUMENT_16: Double,
  FLAG_DOCUMENT_17: Double,
  FLAG_DOCUMENT_18: Double,
  FLAG_DOCUMENT_19: Double,
  FLAG_DOCUMENT_20: Double,
  FLAG_DOCUMENT_21: Double,
  AMT_REQ_CREDIT_BUREAU_HOUR: Double,
  AMT_REQ_CREDIT_BUREAU_DAY: Double,
  AMT_REQ_CREDIT_BUREAU_WEEK: Double,
  AMT_REQ_CREDIT_BUREAU_MON: Double,
  AMT_REQ_CREDIT_BUREAU_QRT: Double,
  AMT_REQ_CREDIT_BUREAU_YEAR: Double,
  AGE: Double,
  YEARS_EMPLOYED: Double,
  YEARS_REGISTERED: Double,
  YEARS_ID_PUBLISH: Double,
  educationOneHot: Array[Double],
  familyStatusOneHot: Array[Double],
  housingTypeOneHot: Array[Double],
  incomeTypeOneHot: Array[Double],
  occupationTypeOneHot: Array[Double]
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

    val processData = rows.flatMap { line =>
  val fields = line.split(",")
  fields match {
    case Array(
          SK_ID_CURR, TARGET, NAME_CONTRACT_TYPE, CODE_GENDER, FLAG_OWN_CAR, FLAG_OWN_REALTY, CNT_CHILDREN, AMT_INCOME_TOTAL, AMT_CREDIT, AMT_ANNUITY, AMT_GOODS_PRICE,
          NAME_INCOME_TYPE, NAME_EDUCATION_TYPE, NAME_FAMILY_STATUS, NAME_HOUSING_TYPE, REGION_POPULATION_RELATIVE, FLAG_EMP_PHONE, FLAG_WORK_PHONE, FLAG_CONT_MOBILE, FLAG_PHONE, FLAG_EMAIL,
          OCCUPATION_TYPE, CNT_FAM_MEMBERS, REGION_RATING_CLIENT, REGION_RATING_CLIENT_W_CITY, REG_REGION_NOT_LIVE_REGION, REG_REGION_NOT_WORK_REGION, LIVE_REGION_NOT_WORK_REGION, REG_CITY_NOT_LIVE_CITY,
          REG_CITY_NOT_WORK_CITY, LIVE_CITY_NOT_WORK_CITY, EXT_SOURCE_2, EXT_SOURCE_3, YEARS_BEGINEXPLUATATION_AVG, FLOORSMAX_AVG, OBS_30_CNT_SOCIAL_CIRCLE, DEF_30_CNT_SOCIAL_CIRCLE,
          OBS_60_CNT_SOCIAL_CIRCLE, DEF_60_CNT_SOCIAL_CIRCLE, DAYS_LAST_PHONE_CHANGE, FLAG_DOCUMENT_2, FLAG_DOCUMENT_3, FLAG_DOCUMENT_4, FLAG_DOCUMENT_5, FLAG_DOCUMENT_6, FLAG_DOCUMENT_7, FLAG_DOCUMENT_8,
          FLAG_DOCUMENT_9, FLAG_DOCUMENT_10, FLAG_DOCUMENT_11, FLAG_DOCUMENT_12, FLAG_DOCUMENT_13, FLAG_DOCUMENT_14, FLAG_DOCUMENT_15, FLAG_DOCUMENT_16, FLAG_DOCUMENT_17, FLAG_DOCUMENT_18, FLAG_DOCUMENT_19,
          FLAG_DOCUMENT_20, FLAG_DOCUMENT_21, AMT_REQ_CREDIT_BUREAU_HOUR, AMT_REQ_CREDIT_BUREAU_DAY, AMT_REQ_CREDIT_BUREAU_WEEK, AMT_REQ_CREDIT_BUREAU_MON, AMT_REQ_CREDIT_BUREAU_QRT, AMT_REQ_CREDIT_BUREAU_YEAR,
          AGE, YEARS_EMPLOYED, YEARS_REGISTERED, YEARS_ID_PUBLISH
        ) =>

        val incomeTypeOneHot = Array(
        if (NAME_INCOME_TYPE == "Working") 1.0 else 0.0,
        if (NAME_INCOME_TYPE == "Commercial associate") 1.0 else 0.0,
        if (NAME_INCOME_TYPE == "Pensioner") 1.0 else 0.0,
        if (NAME_INCOME_TYPE == "State servant") 1.0 else 0.0,
        if (NAME_INCOME_TYPE == "Unemployed") 1.0 else 0.0,
        if (NAME_INCOME_TYPE == "Student") 1.0 else 0.0,
        if (NAME_INCOME_TYPE == "Businessman") 1.0 else 0.0,
        if (NAME_INCOME_TYPE == "Maternity leave") 1.0 else 0.0,
      )

      val educationOneHot = Array(
        if (NAME_EDUCATION_TYPE == "Secondary / secondary special") 1.0 else 0.0,
        if (NAME_EDUCATION_TYPE == "Higher education") 1.0 else 0.0,
        if (NAME_EDUCATION_TYPE == "Incomplete higher") 1.0 else 0.0,
        if (NAME_EDUCATION_TYPE == "Lower secondary") 1.0 else 0.0,
        if (NAME_EDUCATION_TYPE == "Academic degree") 1.0 else 0.0
      )


      val familyStatusOneHot = Array(
        if (NAME_FAMILY_STATUS == "Single / not married") 1.0 else 0.0,
        if (NAME_FAMILY_STATUS == "Married") 1.0 else 0.0,
        if (NAME_FAMILY_STATUS == "Civil marriage") 1.0 else 0.0,
        if (NAME_FAMILY_STATUS == "Widow") 1.0 else 0.0,
        if (NAME_FAMILY_STATUS == "Separated") 1.0 else 0.0,
        if (NAME_FAMILY_STATUS == "Unknown") 1.0 else 0.0
      )


      val housingTypeOneHot = Array(
        if (NAME_HOUSING_TYPE == "House / apartment") 1.0 else 0.0,
        if (NAME_HOUSING_TYPE == "Rented apartment") 1.0 else 0.0,
        if (NAME_HOUSING_TYPE == "With parents") 1.0 else 0.0,
        if (NAME_HOUSING_TYPE == "Municipal apartment") 1.0 else 0.0,
        if (NAME_HOUSING_TYPE == "Office apartment") 1.0 else 0.0,
        if (NAME_HOUSING_TYPE == "Co-op apartment") 1.0 else 0.0
      )

      val occupationTypeOneHot = Array(
        if (OCCUPATION_TYPE == "Laborers") 1.0 else 0.0,
        if (OCCUPATION_TYPE == "Sales staff") 1.0 else 0.0,
        if (OCCUPATION_TYPE == "Core staff") 1.0 else 0.0,
        if (OCCUPATION_TYPE == "Managers") 1.0 else 0.0,
        if (OCCUPATION_TYPE == "Drivers") 1.0 else 0.0,
        if (OCCUPATION_TYPE == "High skill tech staff") 1.0 else 0.0,
        if (OCCUPATION_TYPE == "Accountants") 1.0 else 0.0,
        if (OCCUPATION_TYPE == "Medicine staff") 1.0 else 0.0,
        if (OCCUPATION_TYPE == "Security staff") 1.0 else 0.0,
        if (OCCUPATION_TYPE == "Cooking staff") 1.0 else 0.0,
        if (OCCUPATION_TYPE == "Cleaning staff") 1.0 else 0.0,
        if (OCCUPATION_TYPE == "Private service staff") 1.0 else 0.0,
        if (OCCUPATION_TYPE == "Low-skill Laborers") 1.0 else 0.0,
        if (OCCUPATION_TYPE == "Waiters/barmen staff") 1.0 else 0.0,
        if (OCCUPATION_TYPE == "Secretaries") 1.0 else 0.0,
        if (OCCUPATION_TYPE == "Realty agents") 1.0 else 0.0,
        if (OCCUPATION_TYPE == "HR staff") 1.0 else 0.0,
        if (OCCUPATION_TYPE == "IT staff") 1.0 else 0.0
      )


      Some(LoanApplicant(
        SK_ID_CURR.toDouble,
        TARGET.toDouble,
        if (NAME_CONTRACT_TYPE == "Cash loans") 1.0 else 0.0,
        if (CODE_GENDER == "F") 1.0 else 0.0,
        if (FLAG_OWN_CAR == "Y") 1.0 else 0.0,
        if (FLAG_OWN_REALTY == "Y") 1.0 else 0.0,
        CNT_CHILDREN.toDouble,
        AMT_INCOME_TOTAL.toDouble,
        AMT_CREDIT.toDouble,
        AMT_ANNUITY.toDouble,
        AMT_GOODS_PRICE.toDouble,
        REGION_POPULATION_RELATIVE.toDouble,
        FLAG_EMP_PHONE.toDouble,
        FLAG_WORK_PHONE.toDouble,
        FLAG_CONT_MOBILE.toDouble,
        FLAG_PHONE.toDouble,
        FLAG_EMAIL.toDouble,
        CNT_FAM_MEMBERS.toDouble,
        REGION_RATING_CLIENT.toDouble,
        REGION_RATING_CLIENT_W_CITY.toDouble,
        REG_REGION_NOT_LIVE_REGION.toDouble,
        REG_REGION_NOT_WORK_REGION.toDouble,
        LIVE_REGION_NOT_WORK_REGION.toDouble,
        REG_CITY_NOT_LIVE_CITY.toDouble,
        REG_CITY_NOT_WORK_CITY.toDouble,
        LIVE_CITY_NOT_WORK_CITY.toDouble,
        EXT_SOURCE_2.toDouble,
        EXT_SOURCE_3.toDouble,
        YEARS_BEGINEXPLUATATION_AVG.toDouble,
        FLOORSMAX_AVG.toDouble,
        OBS_30_CNT_SOCIAL_CIRCLE.toDouble,
        DEF_30_CNT_SOCIAL_CIRCLE.toDouble,
        OBS_60_CNT_SOCIAL_CIRCLE.toDouble,
        DEF_60_CNT_SOCIAL_CIRCLE.toDouble,
        DAYS_LAST_PHONE_CHANGE.toDouble,
        FLAG_DOCUMENT_2.toDouble,
        FLAG_DOCUMENT_3.toDouble,
        FLAG_DOCUMENT_4.toDouble,
        FLAG_DOCUMENT_5.toDouble,
        FLAG_DOCUMENT_6.toDouble,
        FLAG_DOCUMENT_7.toDouble,
        FLAG_DOCUMENT_8.toDouble,
        FLAG_DOCUMENT_9.toDouble,
        FLAG_DOCUMENT_10.toDouble,
        FLAG_DOCUMENT_11.toDouble,
        FLAG_DOCUMENT_12.toDouble,
        FLAG_DOCUMENT_13.toDouble,
        FLAG_DOCUMENT_14.toDouble,
        FLAG_DOCUMENT_15.toDouble,
        FLAG_DOCUMENT_16.toDouble,
        FLAG_DOCUMENT_17.toDouble,
        FLAG_DOCUMENT_18.toDouble,
        FLAG_DOCUMENT_19.toDouble,
        FLAG_DOCUMENT_20.toDouble,
        FLAG_DOCUMENT_21.toDouble,
        AMT_REQ_CREDIT_BUREAU_HOUR.toDouble,
        AMT_REQ_CREDIT_BUREAU_DAY.toDouble,
        AMT_REQ_CREDIT_BUREAU_WEEK.toDouble,
        AMT_REQ_CREDIT_BUREAU_MON.toDouble,
        AMT_REQ_CREDIT_BUREAU_QRT.toDouble,
        AMT_REQ_CREDIT_BUREAU_YEAR.toDouble,
        AGE.toDouble,
        YEARS_EMPLOYED.toDouble,
        YEARS_REGISTERED.toDouble,
        YEARS_ID_PUBLISH.toDouble
      ) ++ educationOneHot ++ familyStatusOneHot ++ housingTypeOneHot ++ incomeTypeOneHot ++ occupationTypeOneHot)
    case _ => None
  }
}  

    val clean_data = processData.map(h => h.productIterator.mkString(", "))
    println(clean_data)
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
