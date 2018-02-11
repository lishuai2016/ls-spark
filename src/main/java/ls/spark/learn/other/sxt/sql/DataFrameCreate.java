package ls.spark.learn.other.sxt.sql;//package ls.spark.sxt.sql;
//
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.sql.DataFrame;
//import org.apache.spark.sql.SQLContext;
//
//public class DataFrameCreate {
//
//	public static void main(String[] args) {
//		SparkConf conf = new SparkConf().setAppName("dataframe").setMaster("local");
//		JavaSparkContext sc = new JavaSparkContext(conf);
//		SQLContext sqlContext = new SQLContext(sc);
//
//		// 1.3.1的版本没有read()方法,换新版本
//		DataFrame df = sqlContext.read().json("hdfs://spark001:9000/students.json");
//
//		df.show();
//	}
//}
