package KafkaConsumer

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object WindowDemo3 {
  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("KafkaConsumer.WindowDemo3")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(1))


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

    ssc.checkpoint("hdfs://master:9999/test/streaming/checkpoint")

    //    val text= stream
    //      .map(line => line.value).window(Seconds(6),Seconds(2))
    //    text.print()

    val lines= stream
      .map(line =>
        (line.value.substring(0,15)
          ,line.value.split(",")(2).toInt))
      .reduceByKeyAndWindow((x:Int,y:Int) => x+y,Seconds(10), Seconds(2))
    lines.print()

    ssc.start()
    ssc.awaitTermination()

    ssc.stop(false)
  }
}
