import org.apache.avro.Schema
import org.apache.avro.generic._

object Trade {
  val schema = new Schema.Parser().parse("""
      {
        "namespace": "com.cloudera.spark.examples",
        "type": "record",
        "name": "trade",
        "fields": [
          {"name": "date", "type": "string"},
          {"name": "time", "type": "string"},
          {"name": "price", "type": "double"},
          {"name": "vol", "type": "int"},
          {"name": "type1", "type": "string"},
          {"name": "type2", "type": "string"},
          {"name": "tradeId", "type": "string"}
        ]
      }
    """
    )

  val recordBuilder = new GenericRecordBuilder(schema);
}

class Trade(
  val date: String,
  val time: String,
  val price: Double,
  val vol: Int,
  val type1: String,
  val type2: String,
  val tradeId: String
) {
  def asAvro(): GenericRecord = {
    val builder = Trade.recordBuilder
    builder.set("date", date)
    builder.set("time", time)
    builder.set("price", price)
    builder.set("vol", vol)
    builder.set("type1", type1)
    builder.set("type2", type2)
    builder.set("tradeId", tradeId)
    builder.build
  }
}