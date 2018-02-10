package ls.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

public class Test {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("cache").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// 这个版本是暂时还没有做持久化
		// 1569898	cost 2290 milliseconds
		// 1569898  cost 1078 milliseconds
		
		// 如果做持久化就需要在RDD后面调用cache方法或者persist方法
		// 1569898	cost 2977 milliseconds
		// 1569898	cost 138 milliseconds
		
		// RDD的持久化
		/*
		 * 	Spark非常重要的一个功能就是可以将RDD持久化到内存中
		 *  MEMORY_ONLY
		 */
		JavaRDD<String> lines = sc.textFile("NASA_access_log_Aug95").cache();
		// MEMORY_ONLY
		// MEMORY_ONLY_SER
		// DISK_ONLY_2
		// MEMORY_AND_DISK_SER_2
		// OFF_HEAP tachyon
		
		// 如何选择RDD的持久化策略???
		// 1.Cache() MEMORY_ONLY
		// 2.MEMORY_ONLY_SER
		// 3. _2
		// 4. 能内存不使用磁盘
		
		long beginTime = System.currentTimeMillis();
		long count = lines.count();
		System.out.println(count);
		long endTime = System.currentTimeMillis();
		System.out.println("cost " + (endTime - beginTime) + " milliseconds");
		
		beginTime = System.currentTimeMillis();
		count = lines.count();
		System.out.println(count);
		endTime = System.currentTimeMillis();
		System.out.println("cost " + (endTime - beginTime) + " milliseconds");
		
		sc.close();
	}
}
