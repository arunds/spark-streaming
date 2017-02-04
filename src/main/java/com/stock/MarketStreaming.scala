package com.stock

import scala.util.parsing.json.JSON
import java.sql.DriverManager
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka._
import org.json4s.DefaultFormats
import org.apache.spark.sql.SaveMode

case class Record1(bid_price: Double, order_quantity: Int, symbol: String, trade_type: String, timestamp: String)
case class OutPut(symbol: String, order_quantity: Int)
object MarketStreaming {
    

  def main(args: Array[String]) {

    if (args.length < 4) {
      System.err.println("Usage: JSONStreamExample <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

    StreamingExamples.setStreamingLogLevels() 
    
    println("Step ################### 1");

    val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf().setAppName("JSONStreamExample").setMaster("local[2]").set("spark.driver.allowMultipleContexts", "true")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val sql = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sql)
    println("Step ################### 2");
    val timer = Time(10000)
    //    ssc.checkpoint("checkpoint")

    import sqlContext.implicits._

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    println("Step ################### 3");
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    println("Step ################### 4");
    val jsonf = lines.map(JSON.parseFull(_)).map(_.get.asInstanceOf[scala.collection.immutable.Map[Any, Any]])
    println("Step ################### 5");
    jsonf.print()
    val fields =
      jsonf.map(data => Record1(data("bid_price").toString.toDouble, data("order_quantity").toString.toInt, data("symbol").toString, data("trade_type").toString, data("timestamp").toString))
    fields.print()
    println("Step ################### 6");

    var url = "jdbc:hsqldb:file:C:\\db1\\MarketOrder\\"
    val username = "sa"
    val password = ""
    Class.forName("org.hsqldb.jdbcDriver").newInstance
   

    val results = fields.foreachRDD((recrdd, timer) => {
      println("Step ################### 7");
      recrdd.toDF().registerTempTable("table1");
      val sqlreport = sqlContext.sql("select symbol, SUM(order_quantity) from table1 group by symbol order by symbol")
      sqlreport.map(t => OutPut(t(0).toString, t(1).toString.toInt)).collect().foreach(a => {
        println("Step ################### 8");

         val conn = DriverManager.getConnection(url, username, password)
    val s1 = conn.prepareStatement("CREATE TABLE IF NOT EXISTS MarketOrder2 (symbol VARCHAR(1024), order_quantity NUMERIC, order_time TIMESTAMP )")
    s1.execute
    s1.close
    conn.setAutoCommit(false)
        
        val del = conn.prepareStatement("INSERT INTO MarketOrder2 (symbol, order_quantity, order_time) VALUES (?,?,?) ")
        del.setString(1, a.symbol)
        del.setInt(2, a.order_quantity)
        del.setTimestamp(3, new java.sql.Timestamp(System.currentTimeMillis()));
        del.executeUpdate
        conn.commit
        del.close
        conn.close

        println("Result ########### "+a.symbol + a.order_quantity)
      })
      
      println("Step ################### 9");


      println(sqlreport)


    })
    
    println("Step ################### 10");

    ssc.start()
    ssc.awaitTermination()
    
    println("Step ################### 11");
  }
}