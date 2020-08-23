package ls.spark.core;

import ls.spark.HadoopBase;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

/**
 * 使用本地文件创建RDD
 * 案例：统计文本文件字数
 *
 */
public class LocalFileWordCount  extends HadoopBase{

	public static void main(String[] args) {
		// 创建SparkConf
		SparkConf conf = new SparkConf()
				.setAppName("LocalFile")
				.setMaster("local");
		// 创建JavaSparkContext
		JavaSparkContext sc = new JavaSparkContext(conf);

		// 使用SparkContext以及其子类的textFile()方法，针对本地文件创建RDD
		//JavaRDD<String> lines = sc.textFile("C://Users//Administrator//Desktop//spark.txt");
		JavaRDD<String> lines = sc.textFile("top.txt");

		// 统计文本文件内的字数
		JavaRDD<Integer> lineLength = lines.map(new Function<String, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(String v1) throws Exception {//这里统计的是字符的个数
				return v1.length();
			}

		});

		int count = lineLength.reduce(new Function2<Integer, Integer, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}

		});

		System.out.println("文件总字数是：" + count);

		// 关闭JavaSparkContext
		sc.close();
	}

}
