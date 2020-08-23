package ls.spark.sql;

import ls.spark.HadoopBase;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

/**
 * SaveModel示例
 * @author Administrator
 *
 */
public class SaveModeTest extends HadoopBase{

	@SuppressWarnings("deprecation")
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("SaveModeTest").setMaster("local")
				.set("spark.sql.warehouse.dir", "D:\\spark-warehouse\\");//需要设置一个目录否则报错
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);

		Dataset<Row> peopleDF = sqlContext.read().format("json")
				.load("students.json");
		//生成的文件是json格式的
		peopleDF.write().mode(SaveMode.Append).json("D:\\spark-warehouse\\people_savemode_test");
	}

}
