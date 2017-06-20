  import org.apache.spark.sql.{SQLContext, SparkSession}
  import org.apache.spark.{SparkConf, SparkContext}


  object Payload_Batch{

    def main(args: Array[String]): Unit = {
      if (args.length < 2) {
        System.err.println("Usage: Payload_Batch <local/master> <JSON file with path> ")
        System.exit(1)
      }
      val conf = new SparkConf().setAppName("ReadJson").setMaster(args(0))
      val sc = new SparkContext(conf)
      val sqlContext = new SQLContext(sc)
      val events = sqlContext.read.json(args(1))
      events.createOrReplaceTempView("events")
      val df = sqlContext.sql("select count(event.id), day(CAST(meta.ts_received AS TIMESTAMP)) as Day,hour(CAST(meta.ts_received AS TIMESTAMP)) as Hour,minute(CAST(meta.ts_received AS TIMESTAMP)) as Minute from events GROUP BY day(CAST(meta.ts_received AS TIMESTAMP)),hour(CAST(meta.ts_received AS TIMESTAMP)),minute(CAST(meta.ts_received AS TIMESTAMP)) ORDER BY day(CAST(meta.ts_received AS TIMESTAMP))")

      df.show()
    }
  }
