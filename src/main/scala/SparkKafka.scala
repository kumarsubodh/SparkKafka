/**
  * Created by subodh on 4/13/2016.
  */
package com.capitalone.opentac

import kafka.serializer.StringDecoder

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf


/**
  * Consume message from kafka and reference data from hbase
  * and make decisions based on certain criteria
*/

object sparkKafka {
  def main(args: Array[String]){
    // Define the Kafka parameters, broker list must be specified
    val kafkaParams = Map("metadata.broker.list" -> "sandbox.hortonworks.com:9092")

    // Define which topics to read from
    val topics = Set("opentac-bkup-metric")
    val sparkConf = new SparkConf().setAppName("sparkKafka")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create the direct stream with the Kafka parameters and topics
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)


    val lines = messages.map(_._2)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_+_)

    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
