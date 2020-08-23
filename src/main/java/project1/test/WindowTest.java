package project1.test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class WindowTest {

	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setMaster("local")
				.setAppName("WindowTest");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc.sc());

		List<Tuple2<String, Integer>> grades = Arrays.asList(
				new Tuple2<String, Integer>("class1", 80),
				new Tuple2<String, Integer>("class1", 75),
				new Tuple2<String, Integer>("class1", 90),
				new Tuple2<String, Integer>("class1", 60));
		JavaPairRDD<String, Integer> gradesRDD = sc.parallelizePairs(grades);
		JavaRDD<Row> gradeRowsRDD = gradesRDD.map(new Function<Tuple2<String,Integer>, Row>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Row call(Tuple2<String, Integer> tuple) throws Exception {
				return RowFactory.create(tuple._1, tuple._2);
			}

		});

		StructType schema = DataTypes.createStructType(Arrays.asList(
				DataTypes.createStructField("class", DataTypes.StringType, true),
				DataTypes.createStructField("grade", DataTypes.IntegerType, true)));
		Dataset<Row> gradesDF = sqlContext.createDataFrame(gradeRowsRDD, schema);
		gradesDF.registerTempTable("grades");

		Dataset<Row> gradeLevelDF = sqlContext.sql(
				"SELECT "
				+ "class,"
				+ "grade,"
				+ "row_number() OVER(PARTITION BY class ORDER BY grade DESC) rank "
				+ "FROM grades");

		gradeLevelDF.show();

		sc.close();
	}

}
