package ls.spark.sql;

import ls.spark.HadoopBase;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * 手动指定数据源类型
 * @author Administrator
 *
 */
public class ManuallySpecifyOptions  extends HadoopBase{

	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("ManuallySpecifyOptions")
				.setMaster("local")
				.set("spark.sql.warehouse.dir", "D:\\spark-warehouse\\");//需要设置一个目录否则报错
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);

		Dataset<Row> peopleDF = sqlContext.read().format("json")
				.load("students.json");

		peopleDF.select("name").write().format("parquet")
				.save("D:\\spark-warehouse\\peopleName_java");
	}

}
