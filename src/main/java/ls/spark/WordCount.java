package ls.spark;

import java.util.Arrays;
import java.util.Iterator;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;


/**
 * 集群模式和本地local的区别：
 如果要在spark集群上运行，需要修改的，只有两个地方
 第一，将SparkConf的setMaster()方法给删掉，默认它自己会去连接
 第二，我们针对的不是本地文件了，修改为hadoop hdfs上的真正的存储大数据的文件

 实际执行步骤：
 1、将spark.txt文件上传到hdfs上去
 2、使用我们最早在pom.xml里配置的maven插件，对spark工程进行打包
 3、将打包后的spark工程jar包，上传到机器上执行
 4、编写spark-submit脚本
 5、执行spark-submit脚本，提交spark应用到集群执行
 */

public class WordCount extends HadoopBase {

	public static void main(String[] args) {
		// 第一步：创建SparkConf对象，设置Spark应用的配置信息
		// 使用setMaster()可以设置Spark应用程序要连接的Spark集群的master节点的url
		// 但是如果设置为local则代表，在本地运行
		SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local");
		// 第二步：创建JavaSparkContext对象
		// 在Spark中，SparkContext是Spark所有功能的一个入口，你无论是用java、scala，甚至是python编写
		// 都必须要有一个SparkContext，它的主要作用，包括初始化Spark应用程序所需的一些核心组件，包括
		// 调度器（DAGSchedule、TaskScheduler），还会去到Spark Master节点上进行注册，等等
		// 一句话，SparkContext，是Spark应用中，可以说是最最重要的一个对象
		// 但是呢，在Spark中，编写不同类型的Spark应用程序，使用的SparkContext是不同的，如果使用scala，
		// 使用的就是原生的SparkContext对象
		// 但是如果使用Java，那么就是JavaSparkContext对象
		// 如果是开发Spark SQL程序，那么就是SQLContext、HiveContext
		// 如果是开发Spark Streaming程序，那么就是它独有的SparkContext
		// 以此类推
		JavaSparkContext sc = new JavaSparkContext(conf);

		// 第三步：要针对输入源（hdfs文件、本地文件，等等），创建一个初始的RDD
		// 输入源中的数据会打散，分配到RDD的每个partition中，从而形成一个初始的分布式的数据集
		// 我们这里呢，因为是本地测试，所以呢，就是针对本地文件
		// SparkContext中，用于根据文件类型的输入源创建RDD的方法，叫做textFile()方法
		// 在Java中，创建的普通RDD，都叫做JavaRDD
		// 在这里呢，RDD中，有元素这种概念，如果是hdfs或者本地文件呢，创建的RDD，每一个元素就相当于
		// 是文件里的一行
		JavaRDD<String> lines = sc.textFile("spark.txt");
		// 第四步：对初始RDD进行transformation操作，也就是一些计算操作
		// 通常操作会通过创建function，并配合RDD的map、flatMap等算子来执行
		// function，通常，如果比较简单，则创建指定Function的匿名内部类
		// 但是如果function比较复杂，则会单独创建一个类，作为实现这个function接口的类

		// 先将每一行拆分成单个的单词
		// FlatMapFunction，有两个泛型参数，分别代表了输入和输出类型
		// 我们这里呢，输入肯定是String，因为是一行一行的文本，输出，其实也是String，因为是每一行的文本
		// 这里先简要介绍flatMap算子的作用，其实就是，将RDD的一个元素，给拆分成一个或多个元素
		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>(){

			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<String> call(String line) throws Exception {
				String[] words = line.split(" ");
				return Arrays.asList(words).iterator();

			}

		});

		// 接着，需要将每一个单词，映射为(单词, 1)的这种格式
		// 因为只有这样，后面才能根据单词作为key，来进行每个单词的出现次数的累加
		// mapToPair，其实就是将每个元素，映射为一个(v1,v2)这样的Tuple2类型的元素
		// 如果大家还记得scala里面讲的tuple，那么没错，这里的tuple2就是scala类型，包含了两个值
		// mapToPair这个算子，要求的是与PairFunction配合使用，第一个泛型参数代表了输入类型
		// 第二个和第三个泛型参数，代表的输出的Tuple2的第一个值和第二个值的类型
		// JavaPairRDD的两个泛型参数，分别代表了tuple元素的第一个值和第二个值的类型
		JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String word) throws Exception {
				return new Tuple2<String, Integer>(word, 1);
			}

		});
		// 接着，需要以单词作为key，统计每个单词出现的次数
		// 这里要使用reduceByKey这个算子，对每个key对应的value，都进行reduce操作
		// 比如JavaPairRDD中有几个元素，分别为(hello, 1) (hello, 1) (hello, 1) (world, 1)
		// reduce操作，相当于是把第一个值和第二个值进行计算，然后再将结果与第三个值进行计算
		// 比如这里的hello，那么就相当于是，首先是1 + 1 = 2，然后再将2 + 1 = 3
		// 最后返回的JavaPairRDD中的元素，也是tuple，但是第一个值就是每个key，第二个值就是key的value
		// reduce之后的结果，相当于就是每个单词出现的次数
		JavaPairRDD<String, Integer> wcs = pairs.reduceByKey(new Function2<Integer, Integer, Integer>(){
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		//这里相当于把<key,value>变化为<value,key>，便于下面基于数字进行排序
		JavaPairRDD<Integer, String> tempwcs = wcs.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>(){
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple)
					throws Exception {
				return new Tuple2<Integer, String>(tuple._2(),tuple._1());//调换顺序
			}

		});
		JavaPairRDD<Integer, String> sortedwcs = tempwcs.sortByKey(false);//从大到小逆序排列
		//下面再把顺序调整过来
		JavaPairRDD<String, Integer> resultwcs = sortedwcs.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>(){
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(Tuple2<Integer, String> tuple)
					throws Exception {
				return new Tuple2<String, Integer>(tuple._2(),tuple._1());
			}

		});
		//遍历顺输出
		resultwcs.foreach(new VoidFunction<Tuple2<String, Integer>>(){
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Integer> wc) throws Exception {
				System.out.println(wc._1() + "   " + wc._2());
			}
		});

		sc.close();
	}
}
