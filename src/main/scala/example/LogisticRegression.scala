package example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.feature.VectorAssembler

object LogisticRegression {

  def sigmoid(z: Double): Double = 1.0 / (1.0 + math.exp(-z))

  case class ScaledData(features: Array[Double], label: Double)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("LR").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    val columnNames = Seq(
      "ID", "TARGET", "NAME_CONTRACT_TYPE", "FLAG_OWN_CAR", "FLAG_OWN_REALTY",
      "AMT_INCOME_TOTAL", "AMT_CREDIT", "AMT_ANNUITY", "AMT_GOODS_PRICE",
      "REGION_POPULATION_RELATIVE", "CNT_FAM_MEMBERS", "REGION_RATING_CLIENT",
      "EXT_SOURCE_2", "EXT_SOURCE_3", "YEARS_BEGINEXPLUATATION_AVG",
      "OBS_60_CNT_SOCIAL_CIRCLE", "DEF_60_CNT_SOCIAL_CIRCLE",
      "AMT_REQ_CREDIT_BUREAU_YEAR", "AGE", "YEARS_EMPLOYED",
      "YEARS_REGISTERED", "YEARS_ID_PUBLISH"
    )

    // Load data
    val data = spark.read
      .option("header", "false")
      .option("inferSchema", "true")
      .csv("src/output/part-00000", "src/output/part-00001")
      .toDF(columnNames: _*)

    // Define features and assemble them
    val featureColumns = columnNames.drop(2) // Exclude "ID" and "TARGET"
    val assembler = new VectorAssembler()
      .setInputCols(featureColumns.toArray)
      .setOutputCol("features")

    val assembledData = assembler.transform(data)
      .withColumnRenamed("TARGET", "label")
      .select($"features", $"label")

    // Scale the features based on the mean divided by the standard deviation
    val scaledData = assembledData.rdd.map(row => {
        val features = row.getAs[Vector](0).toArray
        val label = row.getDouble(1)
        val mean = features.sum / features.length
        val std = math.sqrt(features.map(f => math.pow(f - mean, 2)).sum / features.length)
        val scaledFeatures = features.map(f => (f - mean) / std)
        ScaledData(scaledFeatures, label)
    })

    // Define class weights
    val classCounts = scaledData.map(_.label).countByValue()
    val totalSamples = classCounts.values.sum.toDouble
    val numClasses = classCounts.size
    val classWeights = classCounts.map { case (label, count) =>
        label -> math.sqrt(totalSamples / (numClasses * count))
    }.toMap
    
    // Split data into training and validation sets (80/20 split)
    val Array(trainingData, validationData) = scaledData.randomSplit(Array(0.8, 0.2), seed = 42)

    // Convert training data to RDD
    val trainingRDD = trainingData.map(data => (data.features, data.label))

    val learningRate = 0.001
    val maxIterations = 100
    val numFeatures = trainingRDD.first()._1.length
    var weights = Array.fill(numFeatures)(0.0) // Initialize weights to 0

    // Train the model using gradient descent
    for (_ <- 1 to maxIterations) {
      val gradients = trainingRDD.map { case (features, label) =>
        val linearCombination = features.zip(weights).map { case (f, w) => f * w }.sum
        val prediction = sigmoid(linearCombination)
        val error = prediction - label
        val weight = classWeights(label)
        features.map(_ * error * weight)
      }.reduce((g1, g2) => g1.zip(g2).map { case (x, y) => x + y })

      // Update weights
        val regularization = 0.315
        weights = weights.zip(gradients).map { case (w, g) => w - learningRate * g + regularization * w }
    }

    // println(s"Trained weights: ${weights.mkString(", ")}")

    // Evaluate on validation data
    val threshold = 0.3
    val validationRDD = validationData.map(data => (data.features, data.label))
    val predictions = validationRDD.map { case (features, label) =>
      val linearCombination = features.zip(weights).map { case (f, w) => f * w }.sum
      val probability = sigmoid(linearCombination)
      val prediction = if (probability >= threshold) 1.0 else 0.0
      (label, prediction)
    }

    // Calculate accuracy
    val correctPredictions = predictions.filter { case (label, prediction) =>
      label == prediction
    }.count()

    val totalValidation = predictions.count()
    val accuracy = correctPredictions.toDouble / totalValidation

    println(s"Validation Accuracy: $accuracy")

    // Calculate F1 score
    val truePositives = predictions.filter { case (label, prediction) =>
      label == 1.0 && prediction == 1.0
    }.count()

    val falsePositives = predictions.filter { case (label, prediction) =>
      label == 0.0 && prediction == 1.0
    }.count()

    val falseNegatives = predictions.filter { case (label, prediction) =>
      label == 1.0 && prediction == 0.0
    }.count()

    val precision = if (truePositives + falsePositives > 0) {
        truePositives.toDouble / (truePositives + falsePositives)
    } else {
        0.0
    }

    val recall = if (truePositives + falseNegatives > 0) {
        truePositives.toDouble / (truePositives + falseNegatives)
    } else {
        0.0
    }

    val f1Score = if (precision + recall > 0) {
        2 * precision * recall / (precision + recall)
    } else {
        0.0
    }

    println(s"Precision: $precision, Recall: $recall")

    println(s"Validation F1 score: $f1Score")

    spark.stop()
  }
}