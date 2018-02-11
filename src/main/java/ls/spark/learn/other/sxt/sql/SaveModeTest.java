package ls.spark.learn.other.sxt.sql;//package ls.spark.sxt.sql;
//
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.sql.DataFrame;
//import org.apache.spark.sql.SQLContext;
//import org.apache.spark.sql.SaveMode;
//
//public class SaveModeTest {
//
//	@SuppressWarnings("deprecation")
//	public static void main(String[] args) {
//		SparkConf conf = new SparkConf().setAppName("dataframe").setMaster("local");
//		JavaSparkContext sc = new JavaSparkContext(conf);
//		SQLContext sqlContext = new SQLContext(sc);
//
//		DataFrame peopleDF = sqlContext.read().format("json").load("people.json");
//		peopleDF.save("people.json", SaveMode.ErrorIfExists);
//	}
//}
