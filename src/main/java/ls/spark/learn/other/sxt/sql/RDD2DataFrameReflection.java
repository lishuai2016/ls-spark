package ls.spark.learn.other.sxt.sql;//package ls.spark.sxt.sql;
//
//import java.util.List;
//
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.api.java.function.Function;
//import org.apache.spark.sql.DataFrame;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.SQLContext;
//
//public class RDD2DataFrameReflection {
//
//	public static void main(String[] args) {
//		SparkConf conf = new SparkConf().setAppName("dataframe").setMaster("local");
//		JavaSparkContext sc = new JavaSparkContext(conf);
//		SQLContext sqlContext = new SQLContext(sc);
//
//		JavaRDD<String> lines = sc.textFile("students.txt");
//		JavaRDD<Student> students = lines.map(new Function<String, Student>(){
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public Student call(String line) throws Exception {
//				String[] lineSplited = line.split(",");
//				Student stu = new Student();
//				stu.setId(Integer.valueOf(lineSplited[0].trim()));
//				stu.setName(lineSplited[1]);
//				stu.setAge(Integer.valueOf(lineSplited[2].trim()));
//				return stu;
//			}
//
//		});
//
//		// 使用反射方式将RDD转换为DataFrame
//		DataFrame studentDF = sqlContext.createDataFrame(students, Student.class);
//		// 有了DataFrame后就可以注册为一个临时表,SQL语句查询年龄小于等于18的青少年
//		studentDF.registerTempTable("students");
//		DataFrame teenagerDF = sqlContext.sql("select * from students where age <= 18");
//
//		// 再次转回RDD,映射为Student
//		JavaRDD<Row> teenagerRDD = teenagerDF.javaRDD();
//		JavaRDD<Student> teenagerStudentRDD = teenagerRDD.map(new Function<Row, Student>(){
//
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public Student call(Row row) throws Exception {
//				// Row的顺序这里有可能会被打乱
//				System.out.println(row.get(0));
//				System.out.println(row.get(1));
//				System.out.println(row.get(2));
//				int id = row.getInt(1);
//				String name = row.getString(2);
//				int age = row.getInt(0);
//				Student stu = new Student();
//				stu.setId(id);
//				stu.setAge(age);
//				stu.setName(name);
//				return stu;
//			}
//
//		});
//
//		List<Student> studentList = teenagerStudentRDD.collect();
//		for(Student stu : studentList){
//			System.out.println(stu);
//		}
//	}
//
//}
