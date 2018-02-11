package ls.spark.learn.other.sxt;//package ls.spark.sxt;
//
//import java.util.Arrays;
//
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.function.FlatMapFunction;
//import org.apache.spark.api.java.function.Function2;
//import org.apache.spark.api.java.function.PairFunction;
//import org.apache.spark.streaming.Durations;
//import org.apache.spark.streaming.api.java.JavaDStream;
//import org.apache.spark.streaming.api.java.JavaPairDStream;
//import org.apache.spark.streaming.api.java.JavaStreamingContext;
//
//import scala.Tuple2;
//
//public class HDFSWordCount {
//
//	public static void main(String[] args) {
//		SparkConf conf = new SparkConf().setAppName("wordcount").setMaster("local[2]");
//		JavaStreamingContext jssc = new JavaStreamingContext(conf,Durations.seconds(5));
//
//		JavaDStream<String> lines = jssc.textFileStream("hdfs://spark001:9000/wordcount_dir");
//
//		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>(){
//
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public Iterable<String> call(String line) throws Exception {
//			 	return Arrays.asList(line.split(" "));
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
//		jssc.start();
//		jssc.awaitTermination();
//		jssc.stop();
//		jssc.close();
//	}
//}
