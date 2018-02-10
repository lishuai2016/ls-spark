package leap.spark.scala.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * kafka-->spark
  */
object WordCountKafka {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
    //至少2个线程，一个DRecive接受监听端口数据，一个计算
    val sc = new StreamingContext(sparkConf, Durations.seconds(10));

    val topicMap = Map("StringTopic" -> 2)

    val lineRdd = KafkaUtils.createStream(
      sc,
      "172.17.200.153:2181,172.17.201.152:2181,172.17.201.107:2181",
      "WordcountConsumerGroup",
      topicMap
    )

    val wordRdd = lineRdd.flatMap(_._2.split(" "))

    val resultrdd = wordRdd.map { x => (x, 1) }.reduceByKey { _ + _}

    resultrdd.print()
    sc.start()
    sc.awaitTermination()
    sc.stop()
  }
}