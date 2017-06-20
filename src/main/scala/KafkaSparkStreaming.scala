/**
  * Created by Durga Prasad on 20/6/17.
  */

import kafka.serializer.StringDecoder
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object KafkaSparkStreaming {
  def main(args: Array[String]) {

    //All Configuration declar here
    val conf = new SparkConf()
      .setAppName("kafkaevents")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //Declare all the Configs here
    val kafkaTopics = "eventcounttopic"    // command separated list of topics
    val kafkaBrokers = "localhost:9092"   // comma separated list of broker:host
    val batchIntervalSeconds = 2
    val checkpointDir = "/usr/local/Cellar/kafka/0.9.0.1/checkpoint/" //create a checkpoint directory to periodically persist the data

    //If any Spark Streaming Context is present, it kills and launches a new ssc
    val stopActiveContext = true
    if (stopActiveContext) {
      StreamingContext.getActive.foreach { _.stop(stopSparkContext = false) }
    }

    //Create Kafka Stream with the Required Broker and Topic
    def kafkaConsumeStream(ssc: StreamingContext): DStream[(String, String)] = {
      val topicsSet = kafkaTopics.split(",").toSet
      val kafkaParams = Map[String, String]("metadata.broker.list" -> kafkaBrokers)
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        ssc, kafkaParams, topicsSet)
    }

    //Define a spark streaming context with batch interval of 2 seconds
    val ssc = new StreamingContext(sc, Seconds(batchIntervalSeconds))

    // Get the event stream from the Kafka source
    val eventStream = kafkaConsumeStream(ssc).flatMap { event => event._2.split(" ") }

    // Create a stream to do a running count of the events per minute
    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.sum
      val previousCount = state.getOrElse(0)
      Some(currentCount + previousCount)
    }

    //for each events in the data stream count it as 1 for later grouping
    val runningCountStream = eventStream.map { x => (x, 1) }.updateStateByKey(updateFunc)

    // Create temp table at every batch interval
    runningCountStream.foreachRDD { rdd =>
      val sqlContext = SQLContext.getOrCreate(SparkContext.getOrCreate())
      val eventdf = sqlContext.createDataFrame(rdd).toDF("word", "count")
      eventdf.show()


    }

    // To make sure data is not deleted by the time we query it interactively
    ssc.remember(Minutes(1))
    ssc.checkpoint(checkpointDir)
    ssc
    //    }

    // This starts the streaming context in the background.
    ssc.start()

    // This is to ensure that we wait for some time before the background streaming job starts. This will put this cell on hold for 5 times the batchIntervalSeconds.
    ssc.awaitTerminationOrTimeout(batchIntervalSeconds * 5 * 1000)
  }
}