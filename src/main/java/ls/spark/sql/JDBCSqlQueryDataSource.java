package ls.spark.sql;

import ls.spark.HadoopBase;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * JDBC数据源
 * @author Administrator
 * 1、通过编写查询SQL从数据加载数据
 * 2、可以把一些查询结果注册为视图，和其他表来进行关联查询。createOrReplaceTempView
 */
public class JDBCSqlQueryDataSource extends HadoopBase {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("JDBCDataSource").setMaster("local")
				.set("spark.sql.warehouse.dir", "D:\\spark-warehouse\\");//需要设置一个目录否则报错
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);

		// 总结一下
		// jdbc数据源
		// 首先，是通过SQLContext的read系列方法，将mysql中的数据加载为DataFrame
		// 然后可以将DataFrame转换为RDD，使用Spark Core提供的各种算子进行操作
		// 最后可以将得到的数据结果，通过foreach()算子，写入mysql、hbase、redis等等db / cache中

		// 分别将mysql中两张表的数据加载为DataFrame
		Map<String, String> options = new HashMap<String, String>();
		options.put("url", "jdbc:mysql://127.0.0.1:3306/testdb");
		options.put("user","root");
		options.put("password","123456");
		//这里查询结果注册为一个表
		options.put("dbtable", "(select * from student_infos where name = 'a') t1");
		Dataset<Row> studentInfosDF = sqlContext.read().format("jdbc")
				.options(options).load();
		long count = studentInfosDF.count();
		System.out.println("结果行数："+count);
		List<Row> rows = studentInfosDF.collectAsList();
		//这里的Row类似二维表格上的一行，通过位置来输出各个字段的值
		System.out.println("结果："+rows);
		Dataset<Row> select_name_from_t1 = sqlContext.sql("select name from t1");
		select_name_from_t1.show();

		options.put("dbtable", "(select * from student_scores where name = 'a') t2");
		Dataset<Row> studentScoresDF = sqlContext.read().format("jdbc")
				.options(options).load();
		List<Row> rows1 = studentScoresDF.collectAsList();
		System.out.println("结果："+rows1.toString());

		sc.close();
	}

}
