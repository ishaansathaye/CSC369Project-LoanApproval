package example
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.feature.StandardScaler

object LogisticRegression {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("LR").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val schema = Encoders.product[LoanApplicant].schema

    val data = spark.read.option("header", "false").option("inferSchema", "true")
      .schema(schema)
      .csv("src/output/part-00000", "src/output/part-00001")

    data.printSchema()

    // Convert the DataFrame to a Dataset
    import spark.implicits._
    val dataset = data.as[LoanApplicant]

    // Assemble the features to be used by the logistic regression model
    val assembler = new VectorAssembler()
      .setInputCols(Array("NAME_CONTRACT_TYPE",
        "FLAG_OWN_CAR",
        "FLAG_OWN_REALTY",
        "AMT_INCOME_TOTAL",
        "AMT_CREDIT",
        "AMT_ANNUITY",
        "AMT_GOODS_PRICE",
        "REGION_POPULATION_RELATIVE",
        "CNT_FAM_MEMBERS",
        "REGION_RATING_CLIENT",
        "EXT_SOURCE_2",
        "EXT_SOURCE_3",
        "YEARS_BEGINEXPLUATATION_AVG",
        "OBS_60_CNT_SOCIAL_CIRCLE",
        "DEF_60_CNT_SOCIAL_CIRCLE",
        "AMT_REQ_CREDIT_BUREAU_YEAR",
        "AGE",
        "YEARS_EMPLOYED",
        "YEARS_REGISTERED",
        "YEARS_ID_PUBLISH"))
      .setOutputCol("features")

    // Scale the features
    val scaler = new StandardScaler()
        .setInputCol("features")
        .setOutputCol("scaledFeatures")
        .setWithStd(true)
        .setWithMean(true)

    // Calculate the class weights
    val classCounts = dataset.groupBy("TARGET").count().collect()
    val total = classCounts.map(_.getLong(1)).sum.toDouble
    val classWeights = classCounts.map { row =>
        val label = row.getDouble(0)
        val count = row.getLong(1)
        (label, total / (2 * count))
    }.toMap

    // Add the class weights to the DataFrame
    val weightedDataset = dataset.withColumn(
        "classWeight",
        when($"TARGET" === 0.0, classWeights(0.0))
            .otherwise(classWeights(1.0))
        )

    // Define the logistic regression model
    val lr = new LogisticRegression()
      .setLabelCol("TARGET")
      .setFeaturesCol("scaledFeatures")
      .setMaxIter(100)
      .setRegParam(0.3)
      .setWeightCol("classWeight")

    // Prepare the training and test data
    val Array(trainingData, testData) = weightedDataset.randomSplit(Array(0.8, 0.2))

    // Create a Pipeline
    val pipeline = new Pipeline().setStages(Array(assembler, scaler, lr))

    // Train the model
    val model = pipeline.fit(trainingData)

    // Make predictions
    val predictions = model.transform(testData)

    // Select example rows to display
    predictions.filter($"prediction" === 0.0).select("TARGET", "features", "probability", "prediction").show(25)
    predictions.filter($"prediction" === 1.0).select("TARGET", "features", "probability", "prediction").show(25)

    // Evaluate the model using area under ROC
    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol("TARGET")
      .setRawPredictionCol("rawPrediction")
      .setMetricName("areaUnderROC")

    val accuracy = evaluator.evaluate(predictions)
    println(s"Area under ROC = $accuracy")

    sc.stop()
  }
}