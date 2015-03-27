import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object Simple {

  def main(args: Array[String]) {

    val conf = new SparkConf()
	             .setMaster(args(2))
                 .setAppName("Simple")
                 .setJars(Array(args(1)))
                 .set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)
	  val input = sc.textFile(args(0))
	  println(input.count)

  }
}
