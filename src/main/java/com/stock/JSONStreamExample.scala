package com.stock

import java.sql.DriverManager
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Minutes
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Time
import scala.util.parsing.json.JSON

case class Record(bid_price: String, order_quantity: String, symbol: String, trade_type: String)

object JSONStreamExample {

  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: JSONStreamExample <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

    //StreamingExamples.setStreamingLogLevels() 

    val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf().setAppName("JSONStreamExample").setMaster("local[2]").set("spark.driver.allowMultipleContexts", "true")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val sql = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sql)
    val timer = Time(10000)
    ssc.checkpoint("checkpoint")

    import sqlContext._
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)

    val jsonf =
      lines.map(JSON.parseFull(_)).map(_.get.asInstanceOf[scala.collection.immutable.Map[String, Any]])
    val fields =
      jsonf.map(data => Record(data("bid_price").toString, data("order_quantity").toString, data("symbol").toString, data("trade_type").toString))
    fields.print()
    
   
     val stock = sql.parallelize(List(
   ("Apple",  2.0, 10),
   ("Apple",  3.0, 15),
   ("Orange", 5.0, 15),
   ( "Orange", 3.0, 9),
   ( "Orange", 6.0, 18),
   ( "Milk",   5.0, 5)))
   
   
   val result = stock.map{ case ( prod, amt, units) => ((prod), (amt, units)) }.
  reduceByKey((x, y) => (x._1 + y._1, (x._2+ y._2))).collect
   
      System.out.println("# result ## "+result);
  result.foreach(tuple=>println(tuple))
    System.out.println("# result1 ## "+result);
  result.foreach(tuple=>println(tuple))

    val results = fields.foreachRDD(rdd => {
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
          var url = "jdbc:hsqldb:file:C:\\Arun\\Work\\poc\\db\\testdb\\"
          val username = "sa"
          val password = ""
          Class.forName("org.hsqldb.jdbcDriver").newInstance
          val conn = DriverManager.getConnection(url, username, password)
          //val s1 = conn.prepareStatement("CREATE TABLE IF NOT EXISTS MarketOrder (bid_price VARCHAR(1024), order_quantity VARCHAR(1024), symbol VARCHAR(1024), trade_type VARCHAR(1024))")
          val s1 = conn.prepareStatement("CREATE TABLE IF NOT EXISTS STOCK1 (STOCK_NAME VARCHAR(1024))")
          s1.execute
          s1.close
          
          System.out.println("# dbline = 1");

          //val del = conn.prepareStatement("INSERT INTO MarketOrder (bid_price, order_quantity, symbol, trade_type) VALUES (?,?,?,?) ")
            val del = conn.prepareStatement("INSERT INTO STOCK1 (STOCK_NAME) VALUES (?) ")
          //for (bookTitle <- it) { 
          del.setString(1, "countervalue")
          //del.setString(2, data)
          //del.setString(3, data)
          //del.setString(4, data)
          System.out.println("# dbline = 2");
          //del.setString(2, "my input") 
          del.executeUpdate
          System.out.println("# dbline = 3");
          //} 

        })
      })

    })

//    val words = lines.flatMap(_.split(" "))
//    val wordCounts = words.map(x => (x, 1L))
//      .reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(2), 2)
//    wordCounts.print()
//
//    wordCounts.foreachRDD(rdd => {
//      System.out.println("# events = " + rdd.count())
//
//      rdd.foreachPartition(partition => {
//        // Print statements in this section are shown in the executor's stdout logs 
//        //val producer = new KafkaProducer[String, String](props) 
//        partition.foreach(record => {
//          val data = record.toString
//          // As as debugging technique, users can write to DBFS to verify that records are being written out  
//          // dbutils.fs.put("/tmp/test_kafka_output",data,true) 
//          System.out.println("# data = " + data);
//
//          //val url="jdbc:hsqldb:stockdb" 
//          var url = "jdbc:hsqldb:file:C:\\Arun\\Work\\poc\\stockdb\\"
//          val username = "sa"
//          val password = ""
//          Class.forName("org.hsqldb.jdbcDriver").newInstance
//          val conn = DriverManager.getConnection(url, username, password)
//          val s1 = conn.prepareStatement("CREATE TABLE IF NOT EXISTS STOCK (STOCK_NAME VARCHAR(1024))")
//          s1.execute
//          s1.close
//
//          val del = conn.prepareStatement("INSERT INTO STOCK (STOCK_NAME) VALUES (?) ")
//          //for (bookTitle <- it) { 
//          del.setString(1, data)
//          //del.setString(2, "my input") 
//          del.executeUpdate
//          //} 
//
//        })
//      })
//
//    })

    ssc.start()
    ssc.awaitTermination()
  }
}