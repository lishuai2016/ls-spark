package ls.spark.learn.other.sxt;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

public class KafkaReceiverWordCount {

	// ./bin/kafka-topics.sh --zookeeper spark001:2181,spark002:2181,spark003:81 --topic wordcount --replication-factor 1 --partitions 1 --create
	// ./bin/kafka-console-producer.sh --topic wordcount --broker-list spark001:9092,spark002:9092,spark003:9092
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("wordcount").setMaster("local[2]");
		JavaStreamingContext jssc = new JavaStreamingContext(conf,Durations.seconds(5));
		
		// 这个比较重要,是对应你给topic用几个线程去拉取数据
		Map<String, Integer> topicThreadMap = new HashMap<String,Integer>();
		topicThreadMap.put("wordcount20160423", 1);
		
		// kafka这种创建的流,是pair的形式,有俩个值,但第一个值通常都是Null啊
		JavaPairReceiverInputDStream<String, String> lines = KafkaUtils.createStream(
				jssc, 
				"192.168.80.201:2181,192.168.80.202:2181,192.168.80.203:2181", 
				"WordcountConsumerGroup",  
				topicThreadMap);
		
		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<Tuple2<String,String>, String>(){

			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<String> call(Tuple2<String, String> tuple) throws Exception {
			 	return Arrays.asList(tuple._2.split(" ")).iterator();
			}
			
		});
		
		JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>(){

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String word) throws Exception {
				return new Tuple2<String, Integer>(word, 1);
			}
			
		});
		
		JavaPairDStream<String, Integer> wordcounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>(){

			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
			
		});
		
		wordcounts.print();
		
		jssc.start();
		try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		jssc.close();
	}
}
