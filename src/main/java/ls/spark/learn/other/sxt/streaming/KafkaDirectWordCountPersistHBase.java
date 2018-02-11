package ls.spark.learn.other.sxt.streaming;//package ls.spark.sxt.streaming;
//
//import java.util.Arrays;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.Iterator;
//import java.util.Map;
//import java.util.Set;
//
//import kafka.serializer.StringDecoder;
//
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.function.FlatMapFunction;
//import org.apache.spark.api.java.function.Function2;
//import org.apache.spark.api.java.function.PairFunction;
//import org.apache.spark.api.java.function.VoidFunction;
//import org.apache.spark.streaming.Durations;
//import org.apache.spark.streaming.api.java.JavaDStream;
//import org.apache.spark.streaming.api.java.JavaPairDStream;
//import org.apache.spark.streaming.api.java.JavaPairInputDStream;
//import org.apache.spark.streaming.api.java.JavaStreamingContext;
//import org.apache.spark.streaming.kafka.KafkaUtils;
//
//
//import scala.Tuple2;
//
//public class KafkaDirectWordCountPersistHBase {
//
//	public static void main(String[] args) {
//		SparkConf conf = new SparkConf().setAppName("wordcount").setMaster("local[2]");
//		JavaStreamingContext jssc = new JavaStreamingContext(conf,Durations.seconds(5));
//
//		// 首先要创建一份kafka参数map
//		Map<String, String> kafkaParams = new HashMap<String, String>();
//		// 我们这里是不需要zookeeper节点的啊,所以我们这里放broker.list
//		kafkaParams.put("metadata.broker.list",
//				"192.168.80.201:9092,192.168.80.202:9092,192.168.80.203:9092");
//
//		// 然后创建一个set,里面放入你要读取的Topic,这个就是我们所说的,它给你做的很好,可以并行读取多个topic
//		Set<String> topics = new HashSet<String>();
//		topics.add("wordcount");
//
//		JavaPairInputDStream<String,String> lines = KafkaUtils.createDirectStream(
//				jssc,
//				String.class, // key类型
//				String.class, // value类型
//				StringDecoder.class, // 解码器
//				StringDecoder.class,
//				kafkaParams,
//				topics);
//
//		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<Tuple2<String,String>, String>(){
//
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public Iterable<String> call(Tuple2<String,String> tuple) throws Exception {
//			 	return Arrays.asList(tuple._2.split(" "));
//			}
//
//		});
//
//		JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>(){
//
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public Tuple2<String, Integer> call(String word) throws Exception {
//				return new Tuple2<String, Integer>(word, 1);
//			}
//
//		});
//
//		JavaPairDStream<String, Integer> wordcounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>(){
//
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public Integer call(Integer v1, Integer v2) throws Exception {
//				return v1 + v2;
//			}
//
//		});
//
//		wordcounts.print();
//
//		wordcounts.foreachRDD(new VoidFunction<JavaPairRDD<String,Integer>>() {
//
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public void call(JavaPairRDD<String, Integer> wordcountsRDD)
//					throws Exception {
//				wordcountsRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String,Integer>>>() {
//
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public void call(Iterator<Tuple2<String, Integer>> wordcounts) throws Exception {
//						HBaseDAO dao = new HBaseDAOImp();
//						Tuple2<String,Integer> wordcount = null;
//						while(wordcounts.hasNext()){
//							wordcount = wordcounts.next();
//							dao.insert("wordcount", wordcount._1 +"_"+wordcount._2, "f1",
//									new String[]{wordcount._1},
//									new String[]{wordcount._2.toString()}) ;
//						}
//					}
//				});
//			}
//
//
//		});
//
//		jssc.start();
//		jssc.awaitTermination();
//		jssc.close();
//	}
//}
