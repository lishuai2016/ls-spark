package ls.spark.core.upgrade;

import java.util.Arrays;
import java.util.List;

import ls.spark.HadoopBase;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class TakeSampled extends HadoopBase{

	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("Sample")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		List<String> staffList = Arrays.asList("张三", "李四", "王二", "麻子",
				"赵六", "王五", "李大个", "王大妞", "小明", "小倩");
		JavaRDD<String> staffRDD = sc.parallelize(staffList);

		// takeSample算子
		// 与sample不同之处，两点
		// 1、action操作，sample是transformation操作
		// 2、不能指定抽取比例，只能是抽取几个

		List<String> luckyStaffList = staffRDD.takeSample(false, 3);

		for(String luckyStaff : luckyStaffList) {
			System.out.println(luckyStaff);
			/**
			 李大个
			 赵六
			 王大妞
			 */
		}

		sc.close();
	}

}
