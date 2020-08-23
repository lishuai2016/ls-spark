package ls.spark.sql;

import ls.spark.HadoopBase;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * Parquet数据源之自动推断分区
 * @author Administrator
 *
 */
public class ParquetPartitionDiscovery extends HadoopBase{

	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("ParquetPartitionDiscovery")
				.setMaster("local")
				.set("spark.sql.warehouse.dir", "D:\\spark-warehouse\\");//需要设置一个目录否则报错

		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);

		//DataFrame usersDF = sqlContext.read().parquet("hdfs://spark1:9000/spark-study/users/gender=male/country=US/users.parquet");
		Dataset<Row> usersDF = sqlContext.read().parquet(
				"users.parquet");

		usersDF.printSchema();
		/**
		 root
		 |-- name: string (nullable = true)
		 |-- favorite_color: string (nullable = true)
		 |-- favorite_numbers: array (nullable = true)
		 |    |-- element: integer (containsNull = true)
		 */
		usersDF.show();
		/**
		 +------+--------------+----------------+
		 |  name|favorite_color|favorite_numbers|
		 +------+--------------+----------------+
		 |Alyssa|          null|  [3, 9, 15, 20]|
		 |   Ben|           red|              []|
		 +------+--------------+----------------+
		 */
	}

}
