import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object query3 {

  def main(args: Array[String]): Unit = {

    val sparConf = new SparkConf().setMaster("local").setAppName("Query1")

    val sc = new SparkContext(sparConf)

    // File
    val some: RDD[String] = sc.textFile("file:///C:/Users/nickl/OneDrive/Desktop/data/Project3/people_some_infected2.csv")

    // Get each line
    val someLine: RDD[String] = some.flatMap(_.split("\n"))

    // Get infecting people
    val Infected = someLine.filter{ test => {
      val current = test.split(",")
      (current(5) == "Yes")
      }
    }
      // Get the (ID, (x, y))
      .map{ test => {
        val current = test.split(",")
        (current(0), (current(1).toInt, current(2).toInt))
      }
    }

    // Get all people
    val all = someLine.map{ test => {
      val current = test.split(",")
      (current(0), (current(1).toInt, current(2).toInt))
      }
    }

    // Cartesian (Map every infected person with every other person)
    val join = Infected.cartesian(all)

    // Map 1 if people in range of infecting people
    // Map 0 if people is out of range
    val result = join
      .map{
      case(infected, pi) =>{
        val distance = Math.sqrt(Math.pow((pi._2._2 - infected._2._2), 2) + Math.pow((pi._2._1 - infected._2._1), 2))

        println(infected._1)
        if (distance == 0){
          (infected._1, 0)
        }
        else if (distance <= 6){
          (infected._1, 1)
        }
        else{
          (infected._1, 0)
        }
      }
    }

    // Reduce and count number of people in range
    val count = result.reduceByKey(_ + _)

    count.saveAsTextFile("Testing")

  }
}