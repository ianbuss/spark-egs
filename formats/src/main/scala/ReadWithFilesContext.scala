import org.apache.hadoop.fs.Path
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input._

import org.apache.log4j._

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.apache.spark.Partitioner

import scala.collection.JavaConverters._

object HourPartitioner {
  def hourAsLastDirectory(path: String) = {
    val dir = path.substring(0,path.lastIndexOf('/'))
    dir.substring(dir.lastIndexOf('/') + 1).toInt
  }
}

class HourPartitioner(numParts: Int, f: String => Int = HourPartitioner.hourAsLastDirectory) 
  extends Partitioner {
  
  override def numPartitions: Int = numParts

  override def getPartition(key: Any): Int = {
    val hour = f(key.toString)
    hour % numPartitions
  }

  override def equals(other: Any): Boolean = other match {
    case hp: HourPartitioner => hp.numPartitions == numPartitions
    case _ => false
  }
}

object ReadWithFilesContext {
  private[this] val logger = Logger.getLogger(getClass.getName);

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Read data with file context")
    val sc = new SparkContext(conf)
    val job = Job.getInstance

    logger.info("Args: " + args.mkString(" "))

    // Determine the splits (using same approach as Spark)
    FileInputFormat.addInputPath(job, new Path(args(0)))
    val inF = classOf[TextInputFormat].newInstance
    val initialSplits = sc.broadcast(inF.getSplits(job).asScala.toList.map(_.toString))

    // Read the data
    val trades = sc.newAPIHadoopFile(args(0), classOf[TextInputFormat], 
      classOf[LongWritable], classOf[Text])
    
    // Add split context
    val tradesFiles = trades.mapPartitionsWithIndex { 
      (idx, iter) => { 
        val split = initialSplits.value
        iter.map { 
          case (l,t) => (split(idx), t.toString) 
        }
      }
    }

    // Partition according to hour
    val tradesPartitionedByHour = tradesFiles.partitionBy(new HourPartitioner(24))

    // Let's confirm things are partitioned by hour correctly - my test set only has hours 19 & 20
    val partitionCounts = tradesPartitionedByHour.mapPartitionsWithIndex {
      (idx, iter) => {
        iter.map { case (p,t) => (idx, 1) }
      }      
    }

    // Print number of records in each partition
    partitionCounts.reduceByKey(_ + _).collect.foreach(println)
  }
}
