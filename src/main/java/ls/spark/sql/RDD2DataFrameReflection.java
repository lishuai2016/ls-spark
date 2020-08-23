package ls.spark.sql;

import java.util.List;

import ls.spark.HadoopBase;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * 使用反射的方式将RDD转换为DataFrame
 * @author Administrator
 *
 */
public class RDD2DataFrameReflection extends HadoopBase{

	public static void main(String[] args) {
		// 创建普通的RDD
		SparkConf conf = new SparkConf()
				.setMaster("local")
				.setAppName("RDD2DataFrameReflection")
				.set("spark.sql.warehouse.dir", "D:\\spark-warehouse\\");//需要设置一个目录否则报错
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);

		JavaRDD<String> lines = sc.textFile("students.txt");

		JavaRDD<Student> students = lines.map(new Function<String, Student>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Student call(String line) throws Exception {//每一行映射为一个对象
				String[] lineSplited = line.split(",");
				Student stu = new Student();
				stu.setId(Integer.valueOf(lineSplited[0].trim()));
				stu.setName(lineSplited[1]);
				stu.setAge(Integer.valueOf(lineSplited[2].trim()));
				return stu;
			}

		});

		// 使用反射方式，将RDD转换为DataFrame
		// 将Student.class传入进去，其实就是用反射的方式来创建DataFrame
		// 因为Student.class本身就是反射的一个应用
		// 然后底层还得通过对Student Class进行反射，来获取其中的field
		// 这里要求，JavaBean必须实现Serializable接口，是可序列化的
		//DataFrame studentDF = sqlContext.createDataFrame(students, Student.class);
		Dataset<Row> studentDF = sqlContext.createDataFrame(students, Student.class);

		// 拿到了一个DataFrame之后，就可以将其注册为一个临时表，然后针对其中的数据执行SQL语句
		studentDF.registerTempTable("students");

		// 针对students临时表执行SQL语句，查询年龄小于等于18岁的学生，就是teenageer
		//DataFrame teenagerDF = sqlContext.sql("select * from students where age<= 18");
		Dataset<Row> teenagerDF = sqlContext.sql("select * from students where age<= 18");

		// 将查询出来的DataFrame，再次转换为RDD
		JavaRDD<Row> teenagerRDD = teenagerDF.javaRDD();

		// 将RDD中的数据，进行映射，映射为Student
		JavaRDD<Student> teenagerStudentRDD = teenagerRDD.map(new Function<Row, Student>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Student call(Row row) throws Exception {
				// row中的数据的顺序，可能是跟我们期望的是不一样的！
				Student stu = new Student();
				stu.setAge(row.getInt(0));
				stu.setId(row.getInt(1));
				stu.setName(row.getString(2));
				return stu;
			}

		});

		// 将数据collect回来，打印出来
		List<Student> studentList = teenagerStudentRDD.collect();
		for(Student stu : studentList) {
			System.out.println(stu);
		}
	}

}
