package ls.spark.sql;

import ls.spark.HadoopBase;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

/**
 * 通用的load和save操作
 * @author Administrator
 * 把查询结果输出
 */
public class GenericLoadSave extends HadoopBase {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("GenericLoadSave").setMaster("local")
				.set("spark.sql.warehouse.dir", "D:\\spark-warehouse\\");//需要设置一个目录否则报错

		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);

		Dataset<Row> usersDF = sqlContext.read().format("json")
				.load("students.json");

		Dataset<Row> select = usersDF.select("name", "age");

		select.write().mode(SaveMode.Append).json("D:\\spark-warehouse\\test_select");
	}

}
