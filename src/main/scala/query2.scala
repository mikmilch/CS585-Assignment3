import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object query2 {

  def main(args: Array[String]): Unit = {

    val sparConf = new SparkConf().setMaster("local").setAppName("Query1")

    val sc = new SparkContext(sparConf)

    // Files
    val large: RDD[String] = sc.textFile("file:///C:/Users/nickl/OneDrive/Desktop/data/Project3/people_large_test.csv")
    val small: RDD[String] = sc.textFile("file:///C:/Users/nickl/OneDrive/Desktop/data/Project3/infected_small_test.csv")

    // Get each line
    val largeLine: RDD[String] = large.flatMap(_.split("\n"))
    val smallLine: RDD[String] = small.flatMap(_.split("\n"))

    // Get the (ID, (x, y))
    val largeMap = largeLine.map(test => {
      val current = test.split(",")
      (current(0), (current(1).toInt, current(2).toInt))
    })

    val smallMap = smallLine.map(test => {
      val current = test.split(",")
      (current(0), (current(1).toInt, current(2).toInt))
    })

    // Cartesian (Map every infected person with every other person)
    val join = largeMap.cartesian(smallMap)

    // Filter for only those in range
    val filter = join.filter {
      case (pi, infected) => {
        val distance = Math.sqrt(Math.pow((pi._2._2 - infected._2._2), 2) + Math.pow((pi._2._1 - infected._2._1), 2))
        distance <= 6
      }
    }
      // Cannot infect oneself
      .filter{
        case(pi, infected) => {
          pi._1 != infected._1
        }
      }

    // Result (pi, infected)
    val result = filter.map{
      case (pi, infected) => {
        (pi._1, infected._1)
      }
    }

    // List to keep track of all infecting person
    var list: List[Int] = List()

    // Distinct to remove duplicates
    val distinct = result
      .filter{
        case(pi, infected) =>
          list = list.::(pi.toInt)
          println(pi)
          (list.count(x => x == pi.toInt) < 2)
      }

    distinct.saveAsTextFile("query2Test_1")


  }
}
