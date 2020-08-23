package ls.spark.sql;

import ls.spark.HadoopBase;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * DataFrame的常用操作
 * @author Administrator
 *
 */
public class DataFrameOperation extends HadoopBase{

	public static void main(String[] args) {
		// 创建DataFrame
		SparkConf conf = new SparkConf()
				.setAppName("DataFrameCreate").setMaster("local")
				.set("spark.sql.warehouse.dir", "D:\\spark-warehouse\\");//需要设置一个目录否则报错
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);

		// 创建出来的DataFrame完全可以理解为一张表
		Dataset<Row> df = sqlContext.read().json("students.json");

		// 打印DataFrame中所有的数据（select * from ...）
		df.show();
		/**
		 +---+---+-----+
		 |age| id| name|
		 +---+---+-----+
		 | 18|  1|  leo|
		 | 19|  2| jack|
		 | 17|  3|marry|
		 +---+---+-----+
		 */
		// 打印DataFrame的元数据（Schema）
		df.printSchema();
		/**
		 root
		 |-- age: long (nullable = true)
		 |-- id: long (nullable = true)
		 |-- name: string (nullable = true)
		 */
		// 查询某列所有的数据
		df.select("name").show();
		/**
		 +-----+
		 | name|
		 +-----+
		 |  leo|
		 | jack|
		 |marry|
		 +-----+
		 */
		// 查询某几列所有的数据，并对列进行计算
		df.select(df.col("name"), df.col("age").plus(1)).show();
		/**
		 +-----+---------+
		 | name|(age + 1)|
		 +-----+---------+
		 |  leo|       19|
		 | jack|       20|
		 |marry|       18|
		 +-----+---------+
		 */
		// 根据某一列的值进行过滤
		df.filter(df.col("age").gt(18)).show();
		/**
		 +---+---+----+
		 |age| id|name|
		 +---+---+----+
		 | 19|  2|jack|
		 +---+---+----+
		 */
		// 根据某一列进行分组，然后进行聚合
		df.groupBy(df.col("age")).count().show();
		/**
		 +---+-----+
		 |age|count|
		 +---+-----+
		 | 19|    1|
		 | 17|    1|
		 | 18|    1|
		 +---+-----+
		 */
	}

}
