package ls.spark.core;

import ls.spark.HadoopBase;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

/**
 * 取最大的前3个数字
 * 计算topN的逻辑，这里n=3
 */
public class Top3 extends HadoopBase {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("Top3")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> lines = sc.textFile("top.txt");//解析文本中的数据
		//把一行中的数字字符解析为数字
		JavaPairRDD<Integer, String> pairs = lines.mapToPair(

				new PairFunction<String, Integer, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Integer, String> call(String t) throws Exception {
						return new Tuple2<Integer, String>(Integer.valueOf(t), t);
					}

				});

		JavaPairRDD<Integer, String> sortedPairs = pairs.sortByKey(false);//false为降序排列，true为升序排列

		JavaRDD<Integer> sortedNumbers = sortedPairs.map(//只获取key即可

				new Function<Tuple2<Integer,String>, Integer>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Integer call(Tuple2<Integer, String> v1) throws Exception {
						return v1._1;
					}

				});

		List<Integer> sortedNumberList = sortedNumbers.take(3);//截取前N，这里的N=3

		for(Integer num : sortedNumberList) {
			System.out.println(num);
		}

		sc.close();
	}

}
