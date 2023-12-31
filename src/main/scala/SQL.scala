package main.scala
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{avg, ceil, col, collect_list, count, row_number, slice, sort_array, sum, min, max}
//import org.apache.spark.sql.*





object SQL {

  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .config("spark.master", "local")
    .getOrCreate()
  def main(args: Array[String]): Unit = {

    // Files
    val customers = spark.read.option("header", "true").option("inferSchema", "true").csv("C:/Users/nickl/OneDrive/Desktop/data/CustomersTest.csv").toDF()
    val transactions = spark.read.option("header", "true").option("inferSchema", "true").csv("C:/Users/nickl/OneDrive/Desktop/data/TransactionsTest.csv").toDF()

    customers.createOrReplaceTempView("Customers")
    transactions.createOrReplaceTempView("Transactions")

    // Tables
    customers.show()
    transactions.show()
    customers.printSchema()
    transactions.printSchema()


    //Task 2.1) Filter out (drop) the Purchases from P with a total purchase amount above $600. Store the result as T1. (5 points)
    println("\nTask 2.1")
    val T1 = transactions.filter(transactions("TransTotal") < 600) //.createOrReplaceTempView("T1") //.show()
    val T1_1 = T1.createOrReplaceTempView("T1")

    println("T1")
    T1.show()


    //Task 2.2) Group the Purchases in T1 by the Number of Items purchased. For each group
    //calculate the median, min and max of total amount spent for purchases in that group.
    //Report the result back to the client side. (5 points)
    println("\nTask 2.2")

    val T2 = T1.groupBy("TransNumItems").agg(sort_array(collect_list("TransTotal"))(ceil(count("TransTotal")/2)).as("Median"), min("TransTotal").as("Min"), max("TransTotal").as("Max")).sort("TransNumItems") //.show()

    println("T2")
    T2.show()

    //Task 2.3) Group the Purchases in T1 by customer ID only for young customers between
    //18 and 25 years of age. For each group report the customer ID, their age, and total number
    //of items that this person has purchased, and total amount spent by the customer. Store the
    //result as T3. (7 points)
    println("\nTask 2.3")

    val purchasesT1 = T1.select(T1("CustID"), T1("TransTotal"), T1("TransNumItems")) //.show()
    val sumPurchasesT1 = purchasesT1.groupBy("CustID").agg(sum("TransTotal").as("SumTransTotal"), sum("TransNumItems").as("SumTransNumItems")) //.show()

    //NOTE: WE NEED TO SAY THAT OUR DATASET DOESN'T HAVE ANYONE UNDER 18 SO WE DON'T NEED TO FILTER FOR IT
    val customersAge = customers.select(customers("ID"), customers("Age")).filter(customers("Age") < 25) //.show()

    val T3 = sumPurchasesT1.join(customersAge, sumPurchasesT1("CustID") === customersAge("ID")).sort("CustID").drop("ID") //.show()

    println("T3")
    T3.show()




    //Task 2.4) Return all customer pairs IDs (C1 and C2) from T3 such that
    //a. C1 is younger in age than customer C2 and
    //b. C1 spent in total more money than C2 but bought less items.
    //Store the result as T4 and report it back in the form (C1 ID, C2 ID, Age1, Age2,
    //TotalAmount1, TotalAmount2, TotalItemCount1, TotalItemCount2) to the client side. (8
    //points)
    println("\nTask 2.4")

    val T3_1 = T3
    val T3_2 = T3
    val T3Joined = T3_1.as("C1").crossJoin(T3_2.as("C2")) //.show()
    val T3Filtered = T3Joined.filter(T3Joined("C1.Age") < T3Joined("C2.Age")).filter(T3Joined("C1.SumTransTotal") > T3Joined("C2.SumTransTotal")).filter(T3Joined("C1.SumTransNumItems") < T3Joined("C2.SumTransNumItems")) //.show()

    val T4Result = T3Filtered.select("C1.CustID","C2.CustID", "C1.Age", "C2.Age", "C1.SumTransTotal", "C2.SumTransTotal", "C1.SumTransNumItems", "C2.SumTransNumItems")
    val T4 = T4Result.toDF("C1 ID", "C2 ID", "Age1", "Age2", "TotalAmount1", "TotalAmount2", "TotalItemCount1", "TotalItemCount2")

    println("T4")
    T4.show()
  }

}
