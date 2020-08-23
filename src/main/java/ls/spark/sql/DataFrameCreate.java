package ls.spark.sql;

import ls.spark.HadoopBase;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * 使用json文件创建DataFrame
 * @author Administrator
 *
 */
public class DataFrameCreate extends HadoopBase{

	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("DataFrameCreate").setMaster("local")
				.set("spark.sql.warehouse.dir", "D:\\spark-warehouse\\");//需要设置一个目录否则报错
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);

		Dataset<Row> json = sqlContext.jsonFile("students.json");

		json.show();
		/**
		 +---+---+-----+
		 |age| id| name|
		 +---+---+-----+
		 | 18|  1|  leo|
		 | 19|  2| jack|
		 | 17|  3|marry|
		 +---+---+-----+
		 */


	}

}
