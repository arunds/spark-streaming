package com.example

import scala.util.parsing.json.JSON

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.toRDDFunctions


case class Record3(bid_price: Double, order_quantity: Int, symbol: String, trade_type: String, timestamp: String)
case class OutPut2(marketuuid: String, symbol: String, order_quantity: Int)
object StockUpdate {

  def main(args: Array[String]) {
    
    StreamingExamples.setStreamingLogLevels()

    //create streaming context
    val conf = new SparkConf().setAppName("StockUpdate").setMaster("local[2]").set("spark.driver.allowMultipleContexts", "true")
    conf.set("spark.cassandra.connection.host", "127.0.0.1")
    val ssc = new StreamingContext(conf, Seconds(2))

    val sql = new SparkContext(conf)
    val sqlContext = new SQLContext(sql)

    //create DStream and read data from Kafka broker     
    val kafkaStream = KafkaUtils.createStream(ssc, "localhost:2181", "spark-streaming-consumer", Map("spark-topic" -> 1)).map(_._2)
    kafkaStream.print()

    val jsonf = kafkaStream.map(JSON.parseFull(_)).map(_.get.asInstanceOf[scala.collection.immutable.Map[Any, Any]])
    jsonf.print()

    val fields =
      jsonf.map(data => Record3(data("bid_price").toString.toDouble, data("order_quantity").toString.toInt, data("symbol").toString, data("trade_type").toString, data("timestamp").toString))
    fields.print()

    CassandraConnector(conf).withSessionDo { session =>
      session.execute("DROP KEYSPACE IF EXISTS marketspace")
      session.execute("CREATE KEYSPACE marketspace WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
      session.execute("CREATE TABLE marketspace.market_order(marketuuid VARCHAR PRIMARY KEY, symbol VARCHAR, order_quantity INT)")
    }

    import sqlContext.implicits._
    val results = fields.foreachRDD((recrdd) => {
      recrdd.toDF().registerTempTable("table1");
      import com.gilt.timeuuid._
      val sqlreport = sqlContext.sql("select symbol, SUM(order_quantity) from table1 group by symbol order by symbol")
      sqlreport.map(t => OutPut2(TimeUuid().toString(), t(0).toString, t(1).toString.toInt)).saveToCassandra("marketspace", "market_order")
      println(sqlreport)

    })

    ssc.start()
    ssc.awaitTermination()

  }

}