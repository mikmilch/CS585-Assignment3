package main.scala

import main.scala.SQL.spark
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.{DecisionTreeRegressor, LinearRegression, RandomForestRegressor}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum
import org.apache.spark.mllib


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

    val splits = Dataset.randomSplit(Array(0.80, 0.20))
    print("Training\n")
    val Trainset = splits(0).cache() //.show()

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


    linearRegressionModel.transform(assembler.transform(Testset)).show()

    // Train a DecisionTree model.
    val decisionTreeRegression = new DecisionTreeRegressor().setFeaturesCol("features").setLabelCol("TransTotal")

    val decisionTreeRegressionModel = decisionTreeRegression.fit(linear)

    decisionTreeRegressionModel.transform(assembler.transform(Testset)).show()


    // Train a RandomForest model.
    val randomForestRegression = new RandomForestRegressor().setFeaturesCol("features").setLabelCol("TransTotal")

    val randomForestRegressionModel = randomForestRegression.fit(linear)

    randomForestRegressionModel.transform(assembler.transform(Testset)).show()



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




    // Decision Tree Regression


    // Random Forest Regression



  }

}
