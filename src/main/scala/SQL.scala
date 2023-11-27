package main.scala
import org.apache.spark.sql.SparkSession




object SQL {

  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .config("spark.master", "local")
    .getOrCreate()
  def main(args: Array[String]): Unit = {



    val customers = spark.read.option("header", "true").option("inferSchema", "true").csv("C:/Users/nickl/OneDrive/Desktop/data/CustomersTest.csv")
    val transactions = spark.read.option("header", "true").option("inferSchema", "true").csv("C:/Users/nickl/OneDrive/Desktop/data/TransactionsTest.csv")


    customers.show()
    transactions.show()
    transactions.printSchema()


    val T1 = transactions.filter(transactions("TransTotal") < 600).show()

  }

}
