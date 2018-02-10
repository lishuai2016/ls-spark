package ls.spark.simple;

import java.util.*;

import kafka.serializer.StringDecoder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

/**
 * 通过  broker list 的方式连接 kafka不成功
 * kafka-->spark-->console
 */
public class KafkaDirectWordCount {

	public static void main(String[] args) throws InterruptedException {
		SparkConf conf = new SparkConf().setAppName("wordcount").setMaster("local[2]");
		//每隔五秒 封装一个 RDD
		JavaStreamingContext jssc = new JavaStreamingContext(conf,Durations.seconds(5));
		
		// kafka参数map
		Map<String, String> kafkaParams = new HashMap<String, String>();
		// 不使用zookeeper节点,直接使用broker.list
		kafkaParams.put("metadata.broker.list","172.17.201.107:6667,172.17.201.152:6667,172.17.200.153:6667");
		
		// 创建一个set,读取Topic,可以并行读取多个topic
		Set<String> topics = new HashSet<String>();
		topics.add("StringTopic");
		
		JavaPairInputDStream<String,String> lines = KafkaUtils.createDirectStream(
				jssc, 
				String.class, // key类型
				String.class, // value类型
				StringDecoder.class, // 解码器
				StringDecoder.class,
				kafkaParams, 
				topics);
		
		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<Tuple2<String,String>, String>(){

			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<String> call(Tuple2<String, String> tuple2) throws Exception {
				String[] arr = tuple2._2.split(" ");
				List<String> list = Arrays.asList(arr);

				return list.iterator();
			}



//			@Override
//			public Iterable<String> call(Tuple2<String,String> tuple) throws Exception {
//			 	return Arrays.asList(tuple._2.split(" "));
//			}
			
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
		jssc.awaitTermination();
		jssc.close();
	}
}
