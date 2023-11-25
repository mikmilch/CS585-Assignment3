import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object query2 {

  def main(args: Array[String]): Unit = {

    val sparConf = new SparkConf().setMaster("local").setAppName("Query1")

    val sc = new SparkContext(sparConf)

    val large: RDD[String] = sc.textFile("src/main/scala/people_large_test.csv")
    val small: RDD[String] = sc.textFile("src/main/scala/infected_small_test.csv")

    val largeLine: RDD[String] = large.flatMap(_.split("\n"))
    val smallLine: RDD[String] = small.flatMap(_.split("\n"))

    val largeMap = largeLine.map(test => {

      val current = test.split(",")
      (current(0), (current(1).toInt, current(2).toInt))
    })

    val smallMap = smallLine.map(test => {

      val current = test.split(",")
      (current(0), (current(1).toInt, current(2).toInt))
    })

    val join = largeMap.cartesian(smallMap)


    val filter = join.filter {
      case (pi, infected) => {
        val distance = Math.sqrt(Math.pow((pi._2._2 - infected._2._2), 2) + Math.pow((pi._2._1 - infected._2._1), 2))
        distance <= 6
      }
    }

    val result = filter.map{
      case (pi, infected) => {
        (pi._1, infected._1)
      }
    }

    var list: List[Int] = List()

    val distinct = result
      .filter{
        case(pi, infected) =>
          list = list.::(pi.toInt)

          for (test <- list){
            print(test)
          }

          (list.count(x => x == pi.toInt) < 2)
      }

    distinct.saveAsTextFile("query2")


  }
}
