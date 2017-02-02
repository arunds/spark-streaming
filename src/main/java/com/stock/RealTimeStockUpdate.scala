package com.stock

import java.sql.DriverManager
import java.util.HashMap

import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Minutes
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions
import org.apache.spark.streaming.kafka.KafkaUtils


object RealTimeStockUpdate {

  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: RealTimeStockUpdate <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

    //StreamingExamples.setStreamingLogLevels()

    val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf().setAppName("RealTimeStockUpdate").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L))
      .reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(2), 2)
    wordCounts.print()

    wordCounts.foreachRDD(rdd => {
      System.out.println("# events = " + rdd.count())

      rdd.foreachPartition(partition => {
        // Print statements in this section are shown in the executor's stdout logs
        //val producer = new KafkaProducer[String, String](props)
        partition.foreach(record => {
          val data = record.toString
          // As as debugging technique, users can write to DBFS to verify that records are being written out 
          // dbutils.fs.put("/tmp/test_kafka_output",data,true)
          System.out.println("# data = " + data);

          //val url="jdbc:hsqldb:stockdb"
          var url = "jdbc:hsqldb:file:C:\\Arun\\Work\\poc\\stockdb\\"
          val username = "sa"
          val password = ""
          Class.forName("org.hsqldb.jdbcDriver").newInstance
          val conn = DriverManager.getConnection(url, username, password)
          val s1 = conn.prepareStatement("CREATE TABLE IF NOT EXISTS STOCK (STOCK_NAME VARCHAR(1024))")
          s1.execute
          s1.close

          val del = conn.prepareStatement("INSERT INTO STOCK (STOCK_NAME) VALUES (?) ")
          //for (bookTitle <- it) {
          del.setString(1, data)
          //del.setString(2, "my input")
          del.executeUpdate
          //}

        })
      })

    })

    ssc.start()
    ssc.awaitTermination()
  }

}