import org.apache.avro.Schema
import org.apache.avro.generic._

import org.apache.hadoop.mapreduce.Job

import org.apache.log4j._

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import parquet.hadoop.{ParquetOutputFormat, ParquetInputFormat}
import parquet.avro.{AvroParquetOutputFormat, AvroWriteSupport, AvroReadSupport}
import parquet.filter.{RecordFilter, UnboundRecordFilter}

object ParquetTrades {

  private[this] val logger = Logger.getLogger(getClass.getName);

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Parquet Trades")
    val sc = new SparkContext(conf)
    val job = new Job()

    logger.info("Args: " + args(0) + " " + args(1))
    
    // Configure the ParquetOutputFormat to use Avro as the serialization format
    ParquetOutputFormat.setWriteSupportClass(job, classOf[AvroWriteSupport])
    AvroParquetOutputFormat.setSchema(job, Trade.schema)

    val input = sc.textFile(args(0))
    val trades = input.map(t => {
      val f = t.split(",")
      new Trade(
        f(0),
        f(1),
        f(2).toDouble,
        f(3).toInt,
        f(4),
        f(5),
        f(6)
      )
    })
    
    trades.map(t => (null,t.asAvro)).saveAsNewAPIHadoopFile(
      args(1), 
      classOf[Void], 
      classOf[GenericRecord],
      classOf[ParquetOutputFormat[Trade]], 
      job.getConfiguration)

  }
}
