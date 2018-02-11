package ls.spark.learn.other.sxt.sql;//package ls.spark.sxt.sql;
//
//import java.util.ArrayList;
//import java.util.List;
//
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.api.java.function.Function;
//import org.apache.spark.sql.DataFrame;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.RowFactory;
//import org.apache.spark.sql.SQLContext;
//import org.apache.spark.sql.types.DataTypes;
//import org.apache.spark.sql.types.StructField;
//import org.apache.spark.sql.types.StructType;
//
//public class RDD2DataFrameDynamic {
//
//	public static void main(String[] args) {
//		SparkConf conf = new SparkConf().setAppName("dataframe").setMaster("local");
//		JavaSparkContext sc = new JavaSparkContext(conf);
//		SQLContext sqlContext = new SQLContext(sc);
//
//		JavaRDD<String> lines = sc.textFile("students.txt");
//		JavaRDD<Row> rows = lines.map(new Function<String, Row>(){
//
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public Row call(String line) throws Exception {
//				String[] lineSplited = line.split(",");
//				return RowFactory.create(
//						Integer.valueOf(lineSplited[0]),lineSplited[1],Integer.valueOf(lineSplited[2]));
//			}
//
//		});
//
//		// 动态构造元数据,我们没有JavaBean让你反射推断出来有哪些Field,但是我可以通过这种方式fields类型可以从mysql或配置文件中加载出来
//		List<StructField> fields = new ArrayList<StructField>();
//		fields.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));
//		fields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
//		fields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
//
//		StructType structType = DataTypes.createStructType(fields);
//		DataFrame studentDF = sqlContext.createDataFrame(rows, structType);
//		studentDF.registerTempTable("students");
//		DataFrame teenagerDF = sqlContext.sql("select * from students where age <= 18");
//
//		List<Row> teenagerRDD = teenagerDF.javaRDD().collect();
//		for(Row stu : teenagerRDD){
//			System.out.println(stu);
//		}
//	}
//}
