package example
import scala.io._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

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
    
    sc.stop()
  }
}
