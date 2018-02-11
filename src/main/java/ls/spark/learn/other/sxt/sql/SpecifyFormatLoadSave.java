package ls.spark.learn.other.sxt.sql;//package ls.spark.sxt.sql;
//
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.sql.DataFrame;
//import org.apache.spark.sql.SQLContext;
//
//public class SpecifyFormatLoadSave {
//
//	 public static void main(String[] args) {
//		SparkConf conf = new SparkConf().setAppName("dataframe").setMaster("local");
//		JavaSparkContext sc = new JavaSparkContext(conf);
//		SQLContext sqlContext = new SQLContext(sc);
//
//		DataFrame peopleDF = sqlContext.read().format("json").load("people.json");
//		peopleDF.select("name").write().format("parquet").save("peopleName.parquet");
//	}
//}
