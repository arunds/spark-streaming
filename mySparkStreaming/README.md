# Run spark-streaming program


Setup Kafka Topic
-----------------
Start zookeepr
zookeeper-3.4.9\bin>zkServer.cmd

Start Kafka:
.\bin\windows\kafka-server-start.bat .\config\server.properties

Create Topic:
bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic spark-topic

Delete Topic:
bin\windows\kafka-topics.bat --zookeeper localhost:2181 --delete --topic spark-topic

Create Producer:
bin\windows\kafka-topics.bat --zookeeper localhost:2181 --delete --topic spark-topic

Create Consumer:(Optional)
bin\windows\kafka-console-consumer.bat --zookeeper localhost:2181 --topic spark-topic

Start Cassandra
---------------
apache-cassandra-3.10\bin\cassandra.bat



----------------------------------------------------------------------------------------------------------------------------
sample json data to post in producer:
-----------------------------------------------------------------------------------------------------------------------------

{"bid_price":"3","order_quantity":500,"symbol":"Google","trade_type":"limit","timestamp":"2017-02-14T18:45:20.294Z"}
{"bid_price":"3","order_quantity":400,"symbol":"Amazon","trade_type":"limit","timestamp":"2017-02-14T18:45:20.294Z"}
{"bid_price":"3","order_quantity":800,"symbol":"Shell gas","trade_type":"limit","timestamp":"2017-02-14T18:45:20.294Z"}
{"bid_price":"3","order_quantity":600,"symbol":"Cognizant","trade_type":"limit","timestamp":"2017-02-14T18:45:20.294Z"}

{"bid_price":"3","order_quantity":500,"symbol":"Google","trade_type":"limit","timestamp":"2017-02-14T18:45:20.294Z"}
{"bid_price":"3","order_quantity":400,"symbol":"Amazon","trade_type":"limit","timestamp":"2017-02-14T18:45:20.294Z"}
{"bid_price":"3","order_quantity":800,"symbol":"Shell gas","trade_type":"limit","timestamp":"2017-02-14T18:45:20.294Z"}
{"bid_price":"3","order_quantity":600,"symbol":"Cognizant","trade_type":"limit","timestamp":"2017-02-14T18:45:20.294Z"}
{"bid_price":"3","order_quantity":500,"symbol":"Google","trade_type":"limit","timestamp":"2017-02-14T18:45:20.294Z"}
{"bid_price":"3","order_quantity":400,"symbol":"Amazon","trade_type":"limit","timestamp":"2017-02-14T18:45:20.294Z"}
{"bid_price":"3","order_quantity":800,"symbol":"Shell gas","trade_type":"limit","timestamp":"2017-02-14T18:45:20.294Z"}
{"bid_price":"3","order_quantity":600,"symbol":"Cognizant","trade_type":"limit","timestamp":"2017-02-14T18:45:20.294Z"}


