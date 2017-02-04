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
import com.datastax.spark.connector.cql.CassandraConnector
import java.util.UUID

case class Record2(bid_price: Double, order_quantity: Int, symbol: String, trade_type: String, timestamp: String)
case class OutPut1(marketuuid: String, symbol: String, order_quantity: Int)
object SparkCassandraMarketStreaming {

  import com.datastax.spark.connector._

  def main(args: Array[String]) {

    if (args.length < 4) {
      System.err.println("Usage: JSONStreamExample <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

    StreamingExamples.setStreamingLogLevels()


    val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf().setAppName("JSONStreamExample").setMaster("local[2]").set("spark.driver.allowMultipleContexts", "true")
    sparkConf.set("spark.cassandra.connection.host", "127.0.0.1")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val sql = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sql)
    val timer = Time(10000)
    //    ssc.checkpoint("checkpoint")

    import sqlContext.implicits._

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    val jsonf = lines.map(JSON.parseFull(_)).map(_.get.asInstanceOf[scala.collection.immutable.Map[Any, Any]])
    jsonf.print()
    val fields =
      jsonf.map(data => Record2(data("bid_price").toString.toDouble, data("order_quantity").toString.toInt, data("symbol").toString, data("trade_type").toString, data("timestamp").toString))
    fields.print()

    CassandraConnector(sparkConf).withSessionDo { session =>
      session.execute("DROP KEYSPACE IF EXISTS marketspace")
      session.execute("CREATE KEYSPACE marketspace WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
      session.execute("CREATE TABLE marketspace.market_order(marketuuid VARCHAR PRIMARY KEY, symbol VARCHAR, order_quantity INT)")
    }

    val results = fields.foreachRDD((recrdd, timer) => {
      recrdd.toDF().registerTempTable("table1");
       import com.gilt.timeuuid._
      val sqlreport = sqlContext.sql("select symbol, SUM(order_quantity) from table1 group by symbol order by symbol")
      //sqlreport.map(t => OutPut1(t(0).toString, t(1).toString.toInt)).collect().foreach(a => {})
     
      sqlreport.map(t => OutPut1(TimeUuid().toString(), t(0).toString, t(1).toString.toInt)).saveToCassandra("marketspace","market_order")

      println(sqlreport)

    })


    ssc.start()
    ssc.awaitTermination()

  }
}