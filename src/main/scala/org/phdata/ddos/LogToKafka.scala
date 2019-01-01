package org.phdata.ddos

import java.io.{FileNotFoundException, IOException}
import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.log4j.Logger

import scala.io.Source

object LogToKafka {
  def main(args: Array[String]): Unit = {
    val log = Logger.getLogger(getClass.getName)
    val BootstrapServer = ConfigFactory.load().getString("kafka.broker")
    val logPath = ConfigFactory.load().getString("kafka.logPath")
    val topic = ConfigFactory.load().getString("kafka.topic.apacheLogTopic")
    val properties = new Properties()
    properties.put("bootstrap.servers", BootstrapServer)
    properties.put("key.serializer", classOf[StringSerializer])
    properties.put("acks","1")
    properties.put("value.serializer", classOf[StringSerializer])

    /*
    1. Reading Logs from Log File and send each line as topic.
     */
    val producer = new KafkaProducer[String, String](properties)
    try {
      for (line <- Source.fromFile(logPath).getLines) {
        val record = new ProducerRecord(topic, "key", line)
        producer.send(record)
      }

    }catch {
      case e: FileNotFoundException => e.printStackTrace
      case e: IOException => e.printStackTrace
      case e: Exception => e.printStackTrace
        log.info(e.printStackTrace())
    }finally producer.close()
  }
}
