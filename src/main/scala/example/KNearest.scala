package example
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

case class LoanApplicant_KNN(
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

object kHelpers {
  val threshold = 0.55
  // makeModel returns a function that predicts whether or not someone will pay back their loan
  // dependant on four features passed in
  def predict(train_data : RDD[(Double, (Double, Double, Double, Double, Double))],
              predict_tuple : (Double, (Double, Double, Double, Double, Double)),
              k : Int) : Double = {
    val closestPoints = train_data.map(x => (x._1, euclideanDistance(x, predict_tuple))).sortBy(x => (x._2, x._1)).take(k)
    val closestPointsIndex = closestPoints.map(x => x._1)
    val prediction = train_data.filter(x => closestPointsIndex.contains(x._1)).filter(x => x._2._5 == 1.0).count() / k.toDouble
    if(prediction < threshold) 0.0 else 1.0
  }

  //output an rdd with the format
  def predict(train_data : RDD[(Double, (Double, Double, Double, Double, Double))],
              test_data : RDD[(Double, (Double, Double, Double, Double, Double))],
              k : Int) :  RDD[(Double, Double)]  = {
    // print(k + "\r")
    val revised_data = test_data
      // do a cartesian product over all of the data
      .cartesian(train_data)
      // group by the key of the testing data
      .groupBy(x => x._1._1)
      // find the distance for each pair
      .map(x => (x._1, x._2.map(pair => euclideanDistance(pair._1, pair._2))))
    // output an rdd with each prediction
    revised_data.map(x => (x._1, x._2.toList.sorted.take(k).fold(0.0)((total, n) => total + n )))
      .map(x => (x._1, if(x._2 / k.toDouble < threshold) 0.0 else 1.0 ))
  }
  // output of confusion matrix is (True Positives, False Positives, True Negatives, False Negatives
  def makeConfusionMatrix(predicted : RDD[(Double, Double)],
                          actual : RDD[(Double, (Double, Double, Double, Double, Double))]) :
  (Double, Double, Double, Double) = {
    val joined_data = predicted.join(actual)
    // ok so x._2._1 is the actual value, and x._2._2 is the predicted value
    joined_data.map(x => (x._1, (x._2._2._5, x._2._1)))
      .aggregate(0.0, 0.0, 0.0, 0.0)((x, y) => y._2 match
      {case (1.0, 1.0) => (x._1 + 1.0, x._2, x._3, x._4)
        case (1.0, 0.0) => (x._1, x._2 + 1.0, x._3, x._4)
        case (0.0, 0.0) => (x._1, x._2, x._3 + 1.0, x._4)
        case (0.0, 1.0) => (x._1, x._2, x._3, x._4 + 1.0)},
        (x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4))

  }

  def calcMetrics(x : (Double, Double, Double, Double)) : (Double, Double, Double, Double) = {
    val x2 = (((x._1 + x._3) / (x._1 + x._2 + x._3 + x._4)),
      // true positive / (true positive + false positive)
      (x._1 / (x._1 + x._2)),
      // true positive / (true positive + false negative)
      (x._1 / (x._1 + x._4)))
    // output tuple of this is (accuracy, precision, recall, f1)
    (x2._1, x2._2, x2._3, (x2._2 * x2._3) / (x2._2 + x2._3))
  }

  def euclideanDistance(point1 : (Double, (Double, Double, Double, Double, Double)),
                        point2 : (Double, (Double, Double, Double, Double, Double))) : Double = {
    val dist = math.sqrt(math.pow(point1._2._1 - point2._2._1, 2) +
      math.pow(point1._2._2 - point2._2._2, 2) +
      math.pow(point1._2._3 - point2._2._3, 2) +
      math.pow(point1._2._4 - point2._2._4, 2))
    dist
  }
  def scaleFeatures(data : RDD[(Double, (Double, Double, Double, Double, Double))]) :
  RDD[(Double, (Double, Double, Double, Double, Double))] = {
    val feature1_mean = getMean(data.map(x => x._2._1))
    val feature1_std = getStd(data.map(x => x._2._1), feature1_mean)
    val feature2_mean = getMean(data.map(x => x._2._2))
    val feature2_std = getStd(data.map(x => x._2._2), feature2_mean)
    val feature3_mean = getMean(data.map(x => x._2._3))
    val feature3_std = getStd(data.map(x => x._2._3), feature3_mean)
    val feature4_mean = getMean(data.map(x => x._2._4))
    val feature4_std = getStd(data.map(x => x._2._4), feature4_mean)

    data.map(x => (x._1,
      ((x._2._1 - feature1_mean) / feature1_std,
        (x._2._2 - feature2_mean) / feature2_std,
        (x._2._3 - feature3_mean) / feature3_std,
        (x._2._4 - feature4_mean) / feature4_std,
        x._2._5
      )))
  }
  def getMean(data : RDD[Double]) : Double = {
    val mean_tuple = data.aggregate((0.0, 0.0))(
      (x,y) => (x._1 + y, x._2 + 1),
      (x,y) => (x._1 + y._1, x._2 + y._2))
    mean_tuple._1 / mean_tuple._2
  }
  def getStd(data : RDD[Double], mean : Double) : Double = {
    val mean_tuple = data.aggregate((0.0, 0.0))(
      (x,y) => (x._1 + math.pow(y - mean, 2), x._2 + 1),
      (x,y) => (x._1 + y._1, x._2 + y._2))
    math.sqrt(mean_tuple._1 / mean_tuple._2)
  }

}

object KNN {
  def main(args: Array[String]): Unit = {
    // System.setProperty("hadoop.home.dir", "c:/winutils/")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("project").setMaster("local[*]")
    val sc = new SparkContext(conf)


    // LEAVING IN ANALYSIS CODE WITH SWITCH VARIABLES TO CONTROL
    val testing_k = false
    val testing_features = false
    //

    // val data = sc.textFile("/user/isathaye/input/application.csv").persist()
    val data = sc.textFile("./src/application.csv").persist()

    val header = data.first()
    val rows = data.filter(_ != header)

    val processData = rows.flatMap {
      line => val fields = line.split(",")
        fields match {
          case Array(
          id, target, loanType, ownCar, ownRealty, income, credit, annuity, goodsPrice, population, famMembers, regionRating, source2, source3, yearsExpl, obs60, def60, creditCheckYear, age, yearsEmployed, yearsReg, yearsPub) =>


            Some(LoanApplicant_KNN(
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
    val small_data = processData.sample(false, 1)
    val train_data = small_data.sample(false, 0.8)
    val valPlusTest = small_data.subtract(train_data)
    val val_data = valPlusTest.sample(false, 0.5)
    val test_data = valPlusTest.subtract(val_data)
    // restricting myself to four features, as using too many features can
    // make your model worse
    val train_data_1 = kHelpers.scaleFeatures(train_data.map(x => (x.SK_ID_CURR, (x.AMT_INCOME_TOTAL, x.NAME_CONTRACT_TYPE, x.AGE, x.AMT_CREDIT, x.TARGET)))).persist()
    val val_data_1 = kHelpers.scaleFeatures(val_data.map(x => (x.SK_ID_CURR, (x.AMT_INCOME_TOTAL, x.NAME_CONTRACT_TYPE, x.AGE, x.AMT_CREDIT, x.TARGET)))).persist()
    val test_data_1 = kHelpers.scaleFeatures(test_data.map(x => (x.SK_ID_CURR, (x.AMT_INCOME_TOTAL, x.NAME_CONTRACT_TYPE, x.AGE, x.AMT_CREDIT, x.TARGET))))
    val train_data_2 = kHelpers.scaleFeatures(train_data.map(x => (x.SK_ID_CURR, (x.AMT_INCOME_TOTAL, x.FLAG_OWN_CAR, x.CNT_FAM_MEMBERS, x.AMT_ANNUITY, x.TARGET)))).persist()
    val val_data_2 = kHelpers.scaleFeatures(val_data.map(x => (x.SK_ID_CURR, (x.AMT_INCOME_TOTAL, x.FLAG_OWN_CAR, x.CNT_FAM_MEMBERS, x.AMT_ANNUITY, x.TARGET)))).persist()
    val test_data_2 = kHelpers.scaleFeatures(test_data.map(x => (x.SK_ID_CURR, (x.AMT_INCOME_TOTAL, x.FLAG_OWN_CAR, x.CNT_FAM_MEMBERS, x.AMT_ANNUITY, x.TARGET))))
    val train_data_3 = kHelpers.scaleFeatures(train_data.map(x => (x.SK_ID_CURR, (x.AMT_INCOME_TOTAL, x.YEARS_EMPLOYED, x.AGE, x.AMT_CREDIT, x.TARGET)))).persist()
    val val_data_3 = kHelpers.scaleFeatures(val_data.map(x => (x.SK_ID_CURR, (x.AMT_INCOME_TOTAL, x.YEARS_EMPLOYED, x.AGE, x.AMT_CREDIT, x.TARGET)))).persist()
    val test_data_3 = kHelpers.scaleFeatures(test_data.map(x => (x.SK_ID_CURR, (x.AMT_INCOME_TOTAL, x.YEARS_EMPLOYED, x.AGE, x.AMT_CREDIT, x.TARGET))))
    val train_data_4 = kHelpers.scaleFeatures(train_data.map(x => (x.SK_ID_CURR, (x.YEARS_REGISTERED, x.YEARS_EMPLOYED, x.AGE, x.AMT_ANNUITY, x.TARGET)))).persist()
    val val_data_4 = kHelpers.scaleFeatures(val_data.map(x => (x.SK_ID_CURR, (x.YEARS_REGISTERED, x.YEARS_EMPLOYED, x.AGE, x.AMT_ANNUITY, x.TARGET)))).persist()
    val test_data_4 = kHelpers.scaleFeatures(test_data.map(x => (x.SK_ID_CURR, (x.YEARS_REGISTERED, x.YEARS_EMPLOYED, x.AGE, x.AMT_ANNUITY, x.TARGET))))
    val train_data_5 = kHelpers.scaleFeatures(train_data.map(x => (x.SK_ID_CURR, (x.AMT_INCOME_TOTAL, x.YEARS_EMPLOYED, x.YEARS_REGISTERED, x.REGION_RATING_CLIENT, x.TARGET)))).persist()
    val val_data_5 = kHelpers.scaleFeatures(val_data.map(x => (x.SK_ID_CURR, (x.AMT_INCOME_TOTAL, x.YEARS_EMPLOYED, x.YEARS_REGISTERED, x.REGION_RATING_CLIENT, x.TARGET)))).persist()
    val test_data_5 = kHelpers.scaleFeatures(test_data.map(x => (x.SK_ID_CURR, (x.AMT_INCOME_TOTAL, x.YEARS_EMPLOYED, x.YEARS_REGISTERED, x.REGION_RATING_CLIENT, x.TARGET))))

    // this takes the rdd of predictions and joins it back to the test data,
    // and then maps it to whether or not the prediction was accurate
    // this is then aggregated as an accuracy score, which is a rough
    // metric to analyze the data on
    // I will test the accuracy on a few subsets of the dataset
    if(testing_k) {
      val neighbor_list = (1 to 100).toList
      val confusionMatricies = neighbor_list.map(x => (kHelpers.makeConfusionMatrix(kHelpers.predict(train_data_4, val_data_4, x), val_data_4), x))
      // (true positive + true negative) / everything
      val accuracy_precision_recall_k = confusionMatricies.map(x => (((x._1._1 + x._1._3) / (x._1._1 + x._1._2 + x._1._3 + x._1._4)),
        // true positive / (true positive + false positive)
        (x._1._1 / (x._1._1 + x._1._2)),
        // true positive / (true positive + false negative)
        (x._1._1 / (x._1._1 + x._1._4)),
        // k neighbors
        x._2))
      // output tuple of this is (accuracy, precision, recall, f1, k)
      val all_data = accuracy_precision_recall_k.map(x => (x._1, x._2, x._3, (x._2 * x._3) / (x._2 + x._3), x._4))
      // neighbors,accuracy,precision,recall,f1
      // all_data.foreach(x => println(x._5 + "," + x._1 + "," + x._2 + "," + x._3 + "," + x._4))

      // in multiple runs of the data, ~8 to ~12 was the ideal neighbor amount, so I will use 10 nearest neighbors
      all_data.sortWith((x, y) => (x._4 < y._4)).foreach(x => println(x._5 + " : " + x._1 + ", " + x._2 + ", " + x._3 + ", " + x._4))
    }
    if(testing_features) {
      println(kHelpers.calcMetrics(kHelpers.makeConfusionMatrix(kHelpers.predict(train_data_1, val_data_1, 10), val_data_1)))
      println(kHelpers.calcMetrics(kHelpers.makeConfusionMatrix(kHelpers.predict(train_data_2, val_data_2, 10), val_data_2)))
      println(kHelpers.calcMetrics(kHelpers.makeConfusionMatrix(kHelpers.predict(train_data_3, val_data_3, 10), val_data_3)))
      println(kHelpers.calcMetrics(kHelpers.makeConfusionMatrix(kHelpers.predict(train_data_4, val_data_4, 10), val_data_4)))
      println(kHelpers.calcMetrics(kHelpers.makeConfusionMatrix(kHelpers.predict(train_data_5, val_data_5, 10), val_data_5)))
    }

    println(kHelpers.calcMetrics(kHelpers.makeConfusionMatrix(kHelpers.predict(train_data_2, test_data_2, 20), test_data_3)))

  }
}