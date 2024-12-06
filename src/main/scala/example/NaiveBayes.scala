package example
import scala.io._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.math._

object NaiveBayes {
  
  case class NaiveBayesModel(classProbs: Map[Double, Double], featureProbs: Map[Double, Map[Int, (Double, Double)]])

  def calculateMetrics(predictionsAndLabels: RDD[(Double, Double)]): (Double, Double, Double, Double) = {
  val metrics = predictionsAndLabels.map { case (prediction, label) =>
    if (prediction == 1.0 && label == 1.0) ("TP", 1)
    else if (prediction == 1.0 && label == 0.0) ("FP", 1)
    else if (prediction == 0.0 && label == 0.0) ("TN", 1)
    else if (prediction == 0.0 && label == 1.0) ("FN", 1)
    else ("Other", 0) 
  }.reduceByKey(_ + _).collect().toMap

  val TP = metrics.getOrElse("TP", 0)
  val FP = metrics.getOrElse("FP", 0)
  val TN = metrics.getOrElse("TN", 0)
  val FN = metrics.getOrElse("FN", 0)

  val accuracy = if(TP + TN + FP + FN > 0) (TP.toDouble + TN) / (TP + TN + FP + FN) else 0.0
  val precision = if (TP + FP > 0) TP.toDouble / (TP + FP) else 0.0
  val recall = if (TP + FN > 0) TP.toDouble / (TP + FN) else 0.0
  val f1 = if (precision + recall > 0) 2 * (precision * recall) / (precision + recall) else 0.0

  (accuracy, precision, recall, f1)
}

  def calculateFeatureProbs(data: RDD[LoanApplicant]): Map[Double, Map[Int, (Double, Double)]] = {
    val groupedByClass: Map[Double, Iterable[LoanApplicant]] = data.groupBy(_.TARGET).collect().toMap


    groupedByClass.map { case (targetClass, applicants) =>
      val features = applicants.flatMap { applicant =>
        Array(
          applicant.NAME_CONTRACT_TYPE,
          applicant.FLAG_OWN_CAR,
          applicant.FLAG_OWN_REALTY,
          applicant.AMT_INCOME_TOTAL,
          applicant.AMT_CREDIT,
          applicant.AMT_ANNUITY,
          applicant.AMT_GOODS_PRICE,
          applicant.REGION_POPULATION_RELATIVE,
          applicant.CNT_FAM_MEMBERS,
          applicant.REGION_RATING_CLIENT,
          applicant.EXT_SOURCE_2,
          applicant.EXT_SOURCE_3,
          applicant.YEARS_BEGINEXPLUATATION_AVG,
          applicant.OBS_60_CNT_SOCIAL_CIRCLE,
          applicant.DEF_60_CNT_SOCIAL_CIRCLE,
          applicant.AMT_REQ_CREDIT_BUREAU_YEAR,
          applicant.AGE,
          applicant.YEARS_EMPLOYED,
          applicant.YEARS_REGISTERED,
          applicant.YEARS_ID_PUBLISH
        ).zipWithIndex
      }

      val featureStats = features.groupBy(_._2).map { case (featureIndex, featureValues) =>
        val values = featureValues.map(_._1)
        val mean = values.sum / values.size
        val variance = values.map(v => pow(v - mean, 2)).sum / values.size
        (featureIndex, (mean, variance))
      }
      (targetClass, featureStats)
    }
  }

  def trainNaiveBayes(data: RDD[LoanApplicant]): NaiveBayesModel = {
    val classCounts = data.map(_.TARGET).countByValue().toMap
    val totalCount = data.count()

    val classProbs = classCounts.map { case (targetClass, count) =>
      (targetClass, count.toDouble / totalCount)
    }
    val featureProbs = calculateFeatureProbs(data)

    NaiveBayesModel(classProbs, featureProbs)
  }

  def predict(model: NaiveBayesModel, loanApplicant: LoanApplicant): Double = {
    val featureValues = Array(
      loanApplicant.NAME_CONTRACT_TYPE,
      loanApplicant.FLAG_OWN_CAR,
      loanApplicant.FLAG_OWN_REALTY,
      loanApplicant.AMT_INCOME_TOTAL,
      loanApplicant.AMT_CREDIT,
      loanApplicant.AMT_ANNUITY,
      loanApplicant.AMT_GOODS_PRICE,
      loanApplicant.REGION_POPULATION_RELATIVE,
      loanApplicant.CNT_FAM_MEMBERS,
      loanApplicant.REGION_RATING_CLIENT,
      loanApplicant.EXT_SOURCE_2,
      loanApplicant.EXT_SOURCE_3,
      loanApplicant.YEARS_BEGINEXPLUATATION_AVG,
      loanApplicant.OBS_60_CNT_SOCIAL_CIRCLE,
      loanApplicant.DEF_60_CNT_SOCIAL_CIRCLE,
      loanApplicant.AMT_REQ_CREDIT_BUREAU_YEAR,
      loanApplicant.AGE,
      loanApplicant.YEARS_EMPLOYED,
      loanApplicant.YEARS_REGISTERED,
      loanApplicant.YEARS_ID_PUBLISH
    )

    val logLikelihoods = model.classProbs.keys.map { targetClass =>
      val classProb = log(model.classProbs(targetClass))
      val featureLikelihoods = featureValues.zipWithIndex.map { case (featureValue, featureIndex) =>
        val (mean, variance) = model.featureProbs(targetClass)(featureIndex)
        val likelihood = (-0.5 * log(2 * Pi * variance)) - (0.5 * pow(featureValue - mean, 2) / variance)
        likelihood
      }
      val logLikelihood = classProb + featureLikelihoods.sum
      (targetClass, logLikelihood)
    }

    val predictedClass = logLikelihoods.maxBy(_._2)._1
    predictedClass
  }

def loadAndSplitData(sc: SparkContext, path: String): (RDD[LoanApplicant], RDD[LoanApplicant]) = {
  val data = sc.textFile(s"$path/part-*")

  val applicants = data.map { line =>
    val fields = line.split(",").map(_.trim)
    LoanApplicant(
      fields(0).toDouble,
      fields(1).toDouble,
      fields(2).toDouble,
      fields(3).toDouble,
      fields(4).toDouble,
      fields(5).toDouble,
      fields(6).toDouble,
      fields(7).toDouble,
      fields(8).toDouble,
      fields(9).toDouble,
      fields(10).toDouble,
      fields(11).toDouble,
      fields(12).toDouble,
      fields(13).toDouble,
      fields(14).toDouble,
      fields(15).toDouble,
      fields(16).toDouble,
      fields(17).toDouble,
      fields(18).toDouble,
      fields(19).toDouble,
      fields(20).toDouble,
      fields(21).toDouble
    )
  }
  val splits = applicants.randomSplit(Array(0.8, 0.2), seed = 15)
  (splits(0), splits(1))
}


    def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("NaiveBayes").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val (trainData, testData) = loadAndSplitData(sc,  "./src/output")
    println(s"Training Data Count: ${trainData.count()}")
    println(s"Testing Data Count: ${testData.count()}")

    val model = trainNaiveBayes(trainData)

    val predictionsAndLabels: RDD[(Double, Double)] = testData.map { applicant =>
      val prediction = predict(model, applicant)
      (prediction, applicant.TARGET)
    }
    val (accuracy, precision, recall, f1) = calculateMetrics(predictionsAndLabels)
    println(f"Model Accuracy: $accuracy%.3f")
    println(f"Precision: $precision%.3f")
    println(f"Recall: $recall%.3f")
    println(f"F1-Score: $f1%.3f")

    sc.stop()
  }
}
