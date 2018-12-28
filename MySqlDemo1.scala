package KafkaConsumer

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object MySqlDemo1  extends App{
  val sparkConf = new SparkConf().setAppName("KafkaWordCount")
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
  wordCounts.print()
  //数据写入mysql
  import java.sql.DriverManager

  wordCounts.foreachRDD { xrdd =>
    xrdd.foreachPartition { partitionRecords =>
      Class.forName("com.mysql.jdbc.Driver")
      val conn = DriverManager.getConnection("jdbc:mysql://slave1:3306/test", "root", "")
      val statement = conn.prepareStatement(s"insert into kafkatest(dt,n) values (?, ?)")
      partitionRecords.foreach{ case (dt,n) =>
        statement.setString(1, dt)
        statement.setInt(2, n)
        statement.execute()
      }
      statement.close()
      conn.close()
    }
  }

  ssc.start()
  ssc.awaitTermination()
  ssc.stop(false)

}
