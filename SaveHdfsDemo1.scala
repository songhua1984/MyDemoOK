package KafkaConsumer

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object SaveHdfsDemo1 {
  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("KafkaConsumer.SaveHdfsDemo1")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(2))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "master:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test1",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )

    val topics = Array("mytest1")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val wordCounts = stream.map(record => (record.value.substring(0,19),
      record.value.split(",")(2).toInt)).reduceByKey(_ + _)

    wordCounts.repartition(5)
      .saveAsTextFiles("hdfs://master:9999/test/streaming/data/text/wordcount")
    sc.textFile("hdfs://master:9999/test/streaming/data/text/wordcount-154598*/part-00000")
      .coalesce(2)
      .saveAsTextFile("hdfs://master:9999/test/streaming/data/text/wordcount_onefile")



    ssc.start()
    ssc.awaitTermination()

    ssc.stop(false)
  }
}
