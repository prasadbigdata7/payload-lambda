
/**
  * Created by Durga Prasad on 20/6/17.
  */

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Payload_Streaming {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Streaming Json File").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))
    val sqlContext = new SQLContext(sc)


    val textStreamRDD = ssc.textFileStream("/home/plsrao/Desktop/input/")

    val events = textStreamRDD.flatMap(x => x.split(","))
    events.foreachRDD { (rdd: RDD[String], time: Time) =>
      // Get the singleton instance of SparkSession
      val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
      import spark.implicits._

      // Convert RDD[String] to RDD[case class] to DataFrame

      val eventsDataFrame = rdd.map(_.split(",")).map(w => Record2(w(0),w(1).toInt)).toDF()

      //eventsDataFrame.saveAsParquetFile("/home/p1.parquet")

      // Creates a temporary view using the DataFrame
      eventsDataFrame.createOrReplaceTempView("events")

      // Do event count on table using SQL and print it
      val eventCountsDataFrame =
      //    val df = sqlContext.sql("select count(event.id), day(CAST(meta.ts_received AS TIMESTAMP)) as Day,hour(CAST(meta.ts_received AS TIMESTAMP)) as Hour,minute(CAST(meta.ts_received AS TIMESTAMP)) as Minute from events GROUP BY day(CAST(meta.ts_received AS TIMESTAMP)),hour(CAST(meta.ts_received AS TIMESTAMP)),minute(CAST(meta.ts_received AS TIMESTAMP)) ORDER BY day(CAST(meta.ts_received AS TIMESTAMP))")
        spark.sql("select event from events limit 10")

      eventCountsDataFrame.show()
    }

    ssc.start()  // Start the computation

    ssc.awaitTermination()  // Wait for the computation to terminate


  }

}

/** Case class for converting RDD to DataFrame */

case class Record2(event: String, count: Int)

/** Lazily instantiated singleton instance of SparkSession */
object SparkSessionSingleton {

  @transient  private var instance: SparkSession = _

  def getInstance(sparkConf: SparkConf): SparkSession = {
    if (instance == null) {
      instance = SparkSession
        .builder
        .config(sparkConf)
        .getOrCreate()
    }
    instance
  }
}
