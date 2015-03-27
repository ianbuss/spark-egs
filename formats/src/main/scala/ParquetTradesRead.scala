import org.apache.avro.Schema
import org.apache.avro.generic._

import org.apache.hadoop.mapreduce.Job

import org.apache.log4j._

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import parquet.hadoop.{ParquetOutputFormat, ParquetInputFormat}
import parquet.avro.{AvroParquetInputFormat, AvroReadSupport}
import parquet.filter.{RecordFilter, UnboundRecordFilter}

object ParquetTradesRead {

  private[this] val logger = Logger.getLogger(getClass.getName);

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Parquet trades read example")
    val sc = new SparkContext(conf)
    val job = new Job()

    logger.info("Args: " + args.mkString(" "))
    
    // Configure the ParquetOutputFormat to use Avro as the serialization format
    ParquetInputFormat.setReadSupportClass(job, classOf[AvroReadSupport[GenericRecord]])
    AvroParquetInputFormat.setRequestedProjection(job, Trade.schema)

    val input = sc.newAPIHadoopFile(args(0), classOf[ParquetInputFormat[GenericRecord]], 
        classOf[Void], classOf[GenericRecord], job.getConfiguration)
    val trades = input.map(_._2).map(t => {
      new Trade(
        t.get("date").asInstanceOf[String],
        t.get("time").asInstanceOf[String],
        t.get("price").asInstanceOf[Double],
        t.get("vol").asInstanceOf[Int],
        t.get("type1").asInstanceOf[String],
        t.get("type2").asInstanceOf[String],
        t.get("tradeId").asInstanceOf[String]
      )
    })
    
    println(trades.count())

  }
}
