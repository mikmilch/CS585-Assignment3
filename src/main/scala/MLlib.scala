package main.scala

import main.scala.SQL.spark
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.{DecisionTreeRegressor, LinearRegression, RandomForestRegressor}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum
import org.apache.spark.mllib
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.mllib.evaluation.RegressionMetrics


object MLlib {

  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .config("spark.master", "local")
    .getOrCreate()

  def main(args: Array[String]): Unit = {

    val customers = spark.read.option("header", "true").option("inferSchema", "true").csv("C:/Users/nickl/OneDrive/Desktop/data/CustomersTest.csv").toDF()
    val transactions = spark.read.option("header", "true").option("inferSchema", "true").csv("C:/Users/nickl/OneDrive/Desktop/data/TransactionsTest.csv").toDF()

    customers.createOrReplaceTempView("Customers")
    transactions.createOrReplaceTempView("Transactions")

    customers.show()
    transactions.show()
    transactions.printSchema()

    //Task 2.5) Data Preparation 1: Generate a dataset composed of customer ID, TransID,
    //Age, Salary, TransNumItems and TransTotal. Store it as Dataset. (3 points)


    val purchasestransactions = transactions.select(transactions("TransID"), transactions("CustID"), transactions("TransTotal"), transactions("TransNumItems")) //.show()

    //NOTE: WE NEED TO SAY THAT OUR DATASET DOESN'T HAVE ANYONE UNDER 18 SO WE DON'T NEED TO FILTER FOR IT
    val customersAge = customers.select(customers("ID"), customers("Age"), customers("Salary")) //.show()

    val Dataset = purchasestransactions.join(customersAge, purchasestransactions("CustID") === customersAge("ID")).sort("CustID").drop("ID") //.show()




    //Task 2.6) Data Preparation 2: (Randomly) split Dataset into two subsets, namely, Trainset
    //and Testset, such that Trainset contains 80% of Dataset and Testset the remaining 20%. (2
    //points)

    val splits = Dataset.randomSplit(Array(0.80, 0.20), seed = 1)
    print("Training\n")
    val Trainset = splits(0).cache() //.show()

//    Trainset.write.csv("trainset.csv")

    print("Testing\n")
    val Testset = splits(1) //.show()


    //Task 2.7) Predict the price: Identify and train at least 3 machine learning algorithms to
    //predict TransTotal. Specifically, You should treat this problem as a regression model (see
    //https://spark.apache.org/docs/latest/ml-classification-regression.html#regression) in
    //which the "features" (X) for this task are Age, Salary, TransNumItems and the "outcome" (Y)
    //is TransTotal. The algorithms should be trained over Trainset (Task 2.6) and applied
    //(inference) over Testset (Task 2.6). (10 points)

//
    val assembler = new VectorAssembler()
      .setInputCols(Array("Age", "Salary", "TransNumItems"))
      .setOutputCol("features")

    var linear = assembler.transform(Trainset)
    linear = linear.select("features", "TransTotal")

    // Train a Linear Regression model.
    val lr = new LinearRegression().setFeaturesCol("features").setLabelCol("TransTotal")
    val linearRegressionModel = lr.fit(linear)

    val linearPrediction = linearRegressionModel.transform(assembler.transform(Testset)) //.show()

    // Train a DecisionTree model.
    val decisionTreeRegression = new DecisionTreeRegressor().setFeaturesCol("features").setLabelCol("TransTotal")

    val decisionTreeRegressionModel = decisionTreeRegression.fit(linear)

    val decisionTreePrediction = decisionTreeRegressionModel.transform(assembler.transform(Testset)) //.show()


    // Train a RandomForest model.
    val randomForestRegression = new RandomForestRegressor().setFeaturesCol("features").setLabelCol("TransTotal")

    val randomForestRegressionModel = randomForestRegression.fit(linear)

    val randomForestPrediction = randomForestRegressionModel.transform(assembler.transform(Testset)) //.show()



//    val logisticRegression = new LogisticRegression().setFeaturesCol("features").setLabelCol("TransTotal")
//
//    val logisticRegressionModel = logisticRegression.fit(linear)
//
//    logisticRegressionModel.transform(assembler.transform(Testset)).show()


    //Task 2.8) Predict the price: Identify and use at least 3 metrics to evaluate the algorithms
    //from Task 2.7. Note that you should use Regression Evaluation
    //(https://spark.apache.org/docs/1.6.1/mllib-evaluation-metrics.html#regression-model-
    //evaluation). (10 points)

    // Linear Regression

    // Get predictions
//    val valuesAndPreds = linsTotal
//    )
//  }
//
//  // Instantiate metrics object
//  val metrics = new RegressionMetrics(linearPrediction.select("prediction", "TransTotal"));


    // Select the columns "prediction" and "TransTotal" for evaluation
    val RMSEevaluator = new RegressionEvaluator()
      .setLabelCol("TransTotal")
      .setPredictionCol("prediction")
      .setMetricName("rmse") // You can use other metrics like "mse", "mae", "r2"

    val R2evaluator = new RegressionEvaluator()
      .setLabelCol("TransTotal")
      .setPredictionCol("prediction")
      .setMetricName("r2") // You can use other metrics like "mse", "mae", "r2"

    val MAEevaluator = new RegressionEvaluator()
      .setLabelCol("TransTotal")
      .setPredictionCol("prediction")
      .setMetricName("mae") // You can use other metrics like "mse", "mae", "r2"

    // Evaluate the model
    val rmse = RMSEevaluator.evaluate(linearPrediction)
    val r2 = R2evaluator.evaluate(linearPrediction)
    val mae = MAEevaluator.evaluate(linearPrediction)

    // Print the metrics (RMSE, R2, MAE)
    println(s"Linear Regression Evaluation Metrics:")
    println(s"Root Mean Squared Error (RMSE) on test data = $rmse")
    println(s"R-Squared (R2) on test data = $r2")
    println(s"Mean Absolute Error (MAE) on test data = $mae")

    // Decision Tree Regression

    // Evaluate the model
    val rmse_t = RMSEevaluator.evaluate(decisionTreePrediction)
    val r2_t = R2evaluator.evaluate(decisionTreePrediction)
    val mae_t = MAEevaluator.evaluate(decisionTreePrediction)

    // Print the metrics
    println(s"Decision Tree Regression Evaluation Metrics:")
    println(s"Root Mean Squared Error (RMSE) on test data = $rmse_t")
    println(s"R-Squared (R2) on test data = $r2_t")
    println(s"Mean Absolute Error (MAE) on test data = $mae_t")


    // Random Forest Regression

    // Evaluate the model
    val rmse_f = RMSEevaluator.evaluate(randomForestPrediction)
    val r2_f = R2evaluator.evaluate(randomForestPrediction)
    val mae_f = MAEevaluator.evaluate(randomForestPrediction)

    // Print the metrics
    println(s"Random Forest Regression Evaluation Metrics:")
    println(s"Root Mean Squared Error (RMSE) on test data = $rmse_f")
    println(s"R-Squared (R2) on test data = $r2_f")
    println(s"Mean Absolute Error (MAE) on test data = $mae_f")

  }

}
