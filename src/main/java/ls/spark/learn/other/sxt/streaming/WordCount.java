package ls.spark.learn.other.sxt.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * 处理流程:
 * 通过监听master.hadoop", 的9999端口的数据输入，每个10秒切分一个rdd数据输入,处理完毕打印
 */

public class WordCount {

	public static void main(String[] args) throws InterruptedException {
		SparkConf conf = new SparkConf().setAppName("wordcount").setMaster("local[2]");
		// 创建该对象就类似于Spark Core中的JavaSparkContext,类似于Spark SQL中的SQLContext
		// 该对象除了接受SparkConf对象,还要接受一个Batch Interval参数,就是说,每收集多长时间数据划分一个batch去进行处理
		// 这里我们看Durations里面可以设置分钟、毫秒、秒,这里设置一秒 切割rdd
		JavaStreamingContext jssc = new JavaStreamingContext(conf,Durations.seconds(10));
		
		// 首先创建输入DStream,代表一个数据源比如从socket或kafka来持续不断的进入实时数据流
		// 创建一个监听端口的socket数据流,这里面就会有每隔一秒生成一个RDD,RDD的元素类型为String就是一行一行的文本
		JavaReceiverInputDStream<String> lines = jssc.socketTextStream("master.hadoop", 9999);
		// 接着Spark Core提供的算子直接应用在DStream上即可,算子底层会应用在里面的每个RDD上面,RDD转换后的新RDD会作为新DStream中RDD
		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>(){

			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<String> call(String line) throws Exception {
				return Arrays.asList(line.split(" ")).iterator();
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
		
		// 最后每次计算完,都打印一下这一秒钟的单词计数情况,并休眠5秒钟,以便于我们测试和观察
		wordcounts.print();
		
		// 必须调用start方法,整个spark streaming应用才会启动执行,然后卡在那里,最后close释放资源
		jssc.start();
		jssc.awaitTermination();
		jssc.close();
	}
}
