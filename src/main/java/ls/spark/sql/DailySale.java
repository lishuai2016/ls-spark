package ls.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Function1;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @program: ls-spark
 * @author: lishuai
 * @create: 2020-08-22 21:14
 *
 *
object DailySale {

def main(args: Array[String]): Unit = {
val conf = new SparkConf()
.setMaster("local")
.setAppName("DailySale")
val sc = new SparkContext(conf)
val sqlContext = new SQLContext(sc)

import sqlContext.implicits._

// 说明一下，业务的特点
// 实际上呢，我们可以做一个，单独统计网站登录用户的销售额的统计
// 有些时候，会出现日志的上报的错误和异常，比如日志里丢了用户的信息，那么这种，我们就一律不统计了

// 模拟数据
val userSaleLog = Array("2015-10-01,55.05,1122",
"2015-10-01,23.15,1133",
"2015-10-01,15.20,",
"2015-10-02,56.05,1144",
"2015-10-02,78.87,1155",
"2015-10-02,113.02,1123")
val userSaleLogRDD = sc.parallelize(userSaleLog, 5)

// 进行有效销售日志的过滤
val filteredUserSaleLogRDD = userSaleLogRDD
.filter { log => if (log.split(",").length == 3) true else false }

val userSaleLogRowRDD = filteredUserSaleLogRDD
.map { log => Row(log.split(",")(0), log.split(",")(1).toDouble) }

val structType = StructType(Array(
StructField("date", StringType, true),
StructField("sale_amount", DoubleType, true)))

val userSaleLogDF = sqlContext.createDataFrame(userSaleLogRowRDD, structType)

// 开始进行每日销售额的统计
userSaleLogDF.groupBy("date")
.agg('date, sum('sale_amount))
.map { row => Row(row(1), row(2)) }
.collect()
.foreach(println)
}

}
 */
public class DailySale {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("JDBCDataSource").setMaster("local")
                .set("spark.sql.warehouse.dir", "D:\\spark-warehouse\\");//需要设置一个目录否则报错
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        // 说明一下，业务的特点
        // 实际上呢，我们可以做一个，单独统计网站登录用户的销售额的统计
        // 有些时候，会出现日志的上报的错误和异常，比如日志里丢了用户的信息，那么这种，我们就一律不统计了

        // 模拟数据
        List<String> userSaleLog = Arrays.asList("2015-10-01,55.05,1122",
                "2015-10-01,23.15,1133",
                "2015-10-01,15.20,",
                "2015-10-02,56.05,1144",
                "2015-10-02,78.87,1155",
                "2015-10-02,113.02,1123");
        JavaRDD<String> userSaleLogRDD = sc.parallelize(userSaleLog,5);

        // 进行有效销售日志的过滤
        JavaRDD<String> filteredUserSaleLogRDD = userSaleLogRDD.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                return s.split(",").length == 3 ? true :false;
            }
        });

        JavaRDD<Row> userSaleLogRowRDD = filteredUserSaleLogRDD.map(new Function<String, Row>() {
            @Override
            public Row call(String s) throws Exception {
                String[] split = s.split(",");
                return RowFactory.create(split[0],split[1]);
            }
        });

        // 转换为DataFrame
        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("date", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("sale_amount", DataTypes.DoubleType, true));
        StructType structType = DataTypes.createStructType(structFields);

        Dataset<Row> dataFrame = sqlContext.createDataFrame(userSaleLogRowRDD, structType);

        RelationalGroupedDataset relationalGroupedDataset = dataFrame.groupBy("date");
        Dataset<Row> sale_amount = relationalGroupedDataset.sum("sale_amount");

        //todo
    }
}
