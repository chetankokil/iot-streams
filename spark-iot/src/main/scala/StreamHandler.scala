import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._


case class Device(device: String, temp: Double, humidity: Double)

object StreamHandler {

  def main(args: Array[String]) {
      val spark = SparkSession
                    .builder
                    .appName("Stream Handler")
                    .master(sys.env.getOrElse("SPARK_MASTER_URL", "local[*]"))
                    .getOrCreate()

      spark.sparkContext.setLogLevel("INFO")


      import spark.implicits._

      val inputDF = spark
                      .readStream
                      .format("kafka")
                      .option("kafka.bootstrap.servers", "localhost:9092")
                      .option("subscribe", "weather")
                      .option("startingOffsets","latest")
                      .load()
    

      val valueDF = inputDF.selectExpr("CAST(value AS STRING)").as[String]


      // println(valueDF.toString())

      val expandedDF = valueDF.map(v => v.split(","))
        .map(r => Device(
            r(1),
            r(2).toDouble,
            r(3).toDouble
          ))


      val df2 = expandedDF
                  .groupBy("device")
                  .agg(avg("temp"), avg("humidity"))

      val query = df2.writeStream
                          .trigger(Trigger.ProcessingTime("5 seconds"))
                          .outputMode("update")
                          .format("console")
                          .start()

      query.awaitTermination()

  }

}
