package org.phdata.ddos

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaSparkStreaming {
  def main(args: Array[String]): Unit = {
    val ss =SparkSession.builder().appName("NetworkWordCount").master("local[2]").getOrCreate()
    val ssc = new StreamingContext(ss.sparkContext, Seconds(10))
    val BootstrapServer = ConfigFactory.load().getString("kafka.broker")
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> BootstrapServer,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "one",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val properties = new Properties()
    properties.put("bootstrap.servers", BootstrapServer)
    properties.put("key.serializer", classOf[StringSerializer])
    properties.put("value.serializer", classOf[StringSerializer])

    val inputTopic = Array(ConfigFactory.load().getString("kafka.broker.topic.apacheLogTopic"))
    val outputTopic = ConfigFactory.load().getString("kafka.broker.topic.DDoSAttackTopic")
    
    /*
    1. Reading Each Topic from Kafka Broker. 
    2. Splitting topic to get IP Address  
     */

    val stream = KafkaUtils.createDirectStream[String, String](ssc,PreferConsistent,Subscribe[String, String](inputTopic, kafkaParams))
    val pairRecord =  stream.map(record =>{
      val logArray = record.value.split("- -")
      (logArray(0),1)
      }
   )
    /*
    1. Spark Streaming Processing starts here.
    2. DDoS Attack : If getting more than x(Integer) request from a IP address in a Window of X (Second).
    3. Adding TimeStamp of Attacker in Message for further processing.
     */
    
    val window = pairRecord.reduceByKeyAndWindow((a:Int,b:Int) => (a + b),Seconds(30), Seconds(10))
    val attackRDD = window.filter(x =>x._2>=10)
    val attackRDDTimeStamp = attackRDD.map(x => (x._1,x._2,System.currentTimeMillis()))

    /*
    1. Send Message to Kafka Broker with DDoS identified IPs with timestamp.
     */
      val producer = new KafkaProducer[String, String](properties)
      try{
      attackRDDTimeStamp.foreachRDD(rdd => {
        for (item <- rdd.collect()) {
          val message = new ProducerRecord[String, String](outputTopic, item.toString())
          producer.send(message).get().toString
        }
      })
      } catch {
        case e: Exception => e.printStackTrace
      } finally producer.close()

    stream.foreachRDD {
      rdd => val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
