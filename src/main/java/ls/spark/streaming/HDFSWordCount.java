package ls.spark.streaming;

import ls.spark.HadoopBase;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;


/**
 * hdfs从hdfs上读取某个目录下新增的文件,旧文件不读（有问题）
 */
public class HDFSWordCount extends HadoopBase {

	public static void main(String[] args) throws InterruptedException {
		SparkConf conf = new SparkConf().setAppName("wordcount").setMaster("local[2]");
        //每个5秒钟读取一次新增文件
		JavaStreamingContext jssc = new JavaStreamingContext(conf,Durations.seconds(5));

		// 首先，使用JavaStreamingContext的textFileStream()方法，针对HDFS目录创建输入数据流
		JavaDStream<String> lines = jssc.textFileStream("D:\\spark-warehouse\\streaming\\input");

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

		wordcounts.print();

		jssc.start();
		jssc.awaitTermination();
		jssc.stop();
		jssc.close();
	}
}
