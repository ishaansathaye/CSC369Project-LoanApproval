import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object LogisticRegression {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("LR").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val schema = Encoders.product[HealthData].schema

    val data = spark.read.option("header", "false").option("inferSchema", "true")
      .schema(schema)
      .csv("src/output/part-00000", "src/output/part-00001")

    data.printSchema()

    // Convert the DataFrame to a Dataset
    import spark.implicits._
    val dataset = data.as[HealthData]

    // Assemble the features to be used by the logistic regression model
    val assembler = new VectorAssembler()
      .setInputCols(Array("bmi", "smoking", "alcohol", "stroke", "physicalHealth", "mentalHealth", "diffWalking",
        "sex", "age", "race", "diabetic", "activity", "generalHealth", "sleep", "asthma", "kidneyDisease", "skinCancer"))
      .setOutputCol("features")

    // Prepare the training and test data
    val Array(trainingData, testData) = dataset.randomSplit(Array(0.8, 0.2))

    // Define the logistic regression model
    val lr = new LogisticRegression()
      .setLabelCol("heartDisease")
      .setFeaturesCol("features")
      .setMaxIter(10)
      .setRegParam(0.3)

    // Create a Pipeline
    val pipeline = new Pipeline().setStages(Array(assembler, lr))

    // Train the model
    val model = pipeline.fit(trainingData)

    // Make predictions
    val predictions = model.transform(testData)

    // Select example rows to display
    predictions.filter($"prediction" === 1.0).select("heartDisease", "features", "probability", "prediction").show(50)

    // Evaluate the model using area under ROC
    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol("heartDisease")
      .setRawPredictionCol("prediction")
      .setMetricName("areaUnderROC")

    val accuracy = evaluator.evaluate(predictions)
    println(s"Area under ROC = $accuracy")

    sc.stop()
  }
}