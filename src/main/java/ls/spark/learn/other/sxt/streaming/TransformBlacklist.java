package ls.spark.learn.other.sxt.streaming;//package ls.spark.sxt.streaming;
//
//import java.util.ArrayList;
//import java.util.List;
//
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.function.Function;
//import org.apache.spark.api.java.function.PairFunction;
//import org.apache.spark.streaming.Durations;
//import org.apache.spark.streaming.api.java.JavaDStream;
//import org.apache.spark.streaming.api.java.JavaPairDStream;
//import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
//import org.apache.spark.streaming.api.java.JavaStreamingContext;
//
//import com.google.common.base.Optional;
//
//import scala.Tuple2;
//
//public class TransformBlacklist {
//
//	public static void main(String[] args) throws InterruptedException {
//		SparkConf conf = new SparkConf().setAppName("wordcount").setMaster("local[2]");
//		JavaStreamingContext jssc = new JavaStreamingContext(conf,Durations.seconds(5));
//
//		// 用户对我们网站上的广告可以进行点击,点击之后是不是要进行实时计费,但是对于刷广告的人,我们有一个黑名单
//		// 只要是黑名单中的用户点击的广告,我们就给过滤掉
//
//		// 先模拟一份黑名单RDD,true说明启用,false说明不启用
//		List<Tuple2<String,Boolean>> blacklist = new ArrayList<Tuple2<String, Boolean>>();
//		blacklist.add(new Tuple2<String, Boolean>("lily", true));
//
//		@SuppressWarnings("deprecation")
//		final JavaPairRDD<String,Boolean> blacklistRDD = jssc.sc().parallelizePairs(blacklist);
//
//		// 日志本身格式简化,意思到了就可以,就是date username的方式
//		JavaReceiverInputDStream<String> adsClickLogDStream = jssc.socketTextStream("spark001", 9999);
//
//		// 所以要先对输入的数据进行转换操作变成 (username, date username) 以便后面对于数据流中的RDD和定义好的黑名单RDD进行join操作
//		JavaPairDStream<String, String> userAdsClickLogDStream = adsClickLogDStream.mapToPair(
//
//				new PairFunction<String, String, String>(){
//
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public Tuple2<String, String> call(String adsClickLog)
//							throws Exception {
//						return new Tuple2<String,String>(adsClickLog.split(" ")[1], adsClickLog);
//					}
//
//
//				});
//
//		// 实时进行黑名单过滤,执行transform操作,将每个batch的RDD,与黑名单RDD进行join
//		JavaDStream<String> validAdsClickLogDStream = userAdsClickLogDStream.transform(
//				new Function<JavaPairRDD<String,String>, JavaRDD<String>>(){
//
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public JavaRDD<String> call(JavaPairRDD<String, String> userAdsClickLogRDD)
//					throws Exception {
//
//				// 这里为什么是左外连接,因为并不是每个用户都在黑名单中,所以直接用join,那么没有在黑名单中的数据,无法join到就会丢弃
//				// string是用户,string是日志,是否在黑名单里是Optional
//				JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> joinedRDD =
//						 userAdsClickLogRDD.leftOuterJoin(blacklistRDD);
//
//				JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> filteredRDD =
//						joinedRDD.filter(
//								new Function<Tuple2<String,
//									Tuple2<String, Optional<Boolean>>>, Boolean>(){
//
//										private static final long serialVersionUID = 1L;
//
//										@Override
//										public Boolean call(
//												Tuple2<String, Tuple2<String, Optional<Boolean>>> tuple)
//												throws Exception {
//											// 这里tuple就是每个用户对应的访问日志和在黑名单中状态
//											if(tuple._2()._2().isPresent() && tuple._2()._2().get()){
//												return false;
//											}
//											return true;
//										}
//
//						});
//				// 到此为止,filteredRDD中就只剩下没有被过滤的正常用户了,用map函数转换成我们要的格式,我们只要点击日志
//				JavaRDD<String> validAdsClickLogRDD = filteredRDD.map(
//						new Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>,String>(){
//
//							private static final long serialVersionUID = 1L;
//
//							@Override
//							public String call(
//									Tuple2<String, Tuple2<String, Optional<Boolean>>> tuple)
//									throws Exception {
//								return tuple._2()._1();
//							}
//
//						});
//				return validAdsClickLogRDD;
//			}
//
//		});
//
//		// 这后面就可以写入Kafka中间件消息队列,作为广告计费服务的有效广告点击数据
//		validAdsClickLogDStream.print();
//
//		jssc.start();
//		jssc.awaitTermination();
//		jssc.close();
//	}
//}
