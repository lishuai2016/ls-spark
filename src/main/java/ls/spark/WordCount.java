package ls.spark;

import java.util.Arrays;
import java.util.Iterator;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class WordCount {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> lines = sc.textFile("spark.txt");
		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>(){

			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<String> call(String line) throws Exception {
				String[] words = line.split(" ");
				return Arrays.asList(words).iterator();

			}

		});
		JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String word) throws Exception {
				return new Tuple2<String, Integer>(word, 1);
			}

		});
		JavaPairRDD<String, Integer> wcs = pairs.reduceByKey(new Function2<Integer, Integer, Integer>(){
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		JavaPairRDD<Integer, String> tempwcs = wcs.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>(){
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple)
					throws Exception {
				return new Tuple2<Integer, String>(tuple._2(),tuple._1());
			}

		});
		JavaPairRDD<Integer, String> sortedwcs = tempwcs.sortByKey(false);
		JavaPairRDD<String, Integer> resultwcs = sortedwcs.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>(){
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(Tuple2<Integer, String> tuple)
					throws Exception {
				return new Tuple2<String, Integer>(tuple._2(),tuple._1());
			}

		});
		resultwcs.foreach(new VoidFunction<Tuple2<String, Integer>>(){
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Integer> wc) throws Exception {
				System.out.println(wc._1() + "   " + wc._2());
			}
		});

		sc.close();
	}
}
