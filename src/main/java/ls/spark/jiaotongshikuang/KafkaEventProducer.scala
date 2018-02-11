package ls.spark.jiaotongshikuang


import java.util.Properties

import kafka.javaapi.producer.Producer
import kafka.producer.{KeyedMessage, ProducerConfig}
import org.apache.spark.{SparkContext, SparkConf}
import org.codehaus.jettison.json.JSONObject



/**
 * 产生数据到kafka中
 */
object KafkaEventProducer {

  // bin/kafka-topics.sh --zookeeper spark001:2181 --create --topic user_events --replication-factor 2 --partitions 2
  // bin/kafka-topics.sh --zookeeper spark001:2181 --list
  // bin/kafka-topics.sh --zookeeper spark001:2181  --describe user_events
  def main(args: Array[String]): Unit = {
    val topic = "car_events"
    val brokers = "192.168.80.201:9092,192.168.80.202:9092,192.168.80.203:9092"
    val props = new Properties()
    props.put("metadata.broker.list", brokers)
    props.put("serializer.class", "kafka.serializer.StringEncoder")

    val kafkaConfig = new ProducerConfig(props)
    val producer = new Producer[String, String](kafkaConfig)

    val sparkConf = new SparkConf().setAppName("Beijing traffic").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)

//    val filePath = "D:/traffic/trafficlf_all_column_all.txt"
    val filePath = "data/2014082013_all_column_test.txt"

    val records = sc.textFile(filePath)
            .filter(!_.startsWith(";"))
                .map(_.split(",")).collect()

    for(record <- records){
      // prepare event data
      val event = new JSONObject()
      event
              .put("camera_id", record(0))
              .put("car_id", record(2))
              .put("event_time", record(4))
              .put("speed", record(6))
              .put("road_id", record(13))

      // produce event message
      producer.send(new KeyedMessage[String, String](topic, event.toString))
      println("Message sent: " + event)

      Thread.sleep(200)
    }
  }
}