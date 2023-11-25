import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object query3 {

  def main(args: Array[String]): Unit = {

    val sparConf = new SparkConf().setMaster("local").setAppName("Query1")

    val sc = new SparkContext(sparConf)

    val some: RDD[String] = sc.textFile("src/main/scala/people_some_infected_test.csv")

    val someLine: RDD[String] = some.flatMap(_.split("\n"))

    val Infected = someLine.filter{ test => {
      val current = test.split(",")
      (current(5) == "Yes")
    }
    }
      .map{ test => {
        val current = test.split(",")
        (current(0), (current(1).toInt, current(2).toInt))
      }
    }

    val all = someLine.map{ test => {
      val current = test.split(",")
      (current(0), (current(1).toInt, current(2).toInt))
    }
    }


    val join = Infected.cartesian(all)

    val result = join.filter{
      case(infected, pi) => {
        val distance = Math.sqrt(Math.pow((pi._2._2 - infected._2._2), 2) + Math.pow((pi._2._1 - infected._2._1), 2))
        distance <= 6
    }
    }
      .map{
        case(infected, pi) => {
          (infected._1, 1)
        }
      }

    val count = result.reduceByKey(_ + _)

    count.saveAsTextFile("Query3")

  }
}