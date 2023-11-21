import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object query1 {

  def main(args: Array[String]): Unit = {

    val sparConf = new SparkConf().setMaster("local").setAppName("Query1")

    val sc = new SparkContext(sparConf)

    val large: RDD[String] = sc.textFile("src/main/scala/people_large_test.csv")
    val small: RDD[String] = sc.textFile("src/main/scala/infected_small_test.csv")

    val counts = large.flatMap(line => line.split(","))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    counts.saveAsTextFile("test.csv")




  }

}
