package ls.spark.ksh;


import hbase.impl.HbaseDaoImpl;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;

/**
 * Created by FromX on 2017/3/17.
 * kafka -->spark-->hbase
 */
public class KSHWordCount {

    public static void main(String[] args) throws InterruptedException {

        SparkConf conf = new SparkConf().setAppName("wc").setMaster("local[2]");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));


        Map<String, Integer> topicThreadMap = new HashMap<String, Integer>();
        // topic名字  线程数 2
        topicThreadMap.put("TestStringTopic", 2);

        // kafka这种创建的流,是pair的形式,有俩个值,但第一个值通常都是Null啊
        JavaPairReceiverInputDStream<String, String> lines = KafkaUtils.createStream(
                jssc,
                "m1.wb.com:2181,c2.wb.com:2181,c1.wb.com:2181",
                "WordcountConsumerGroup",
                topicThreadMap);

        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Iterator<String> call(Tuple2<String, String> tuple2) throws Exception {
                return Arrays.asList(tuple2._2().split(" ")).iterator();
            }

        });

        JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }

        });

        JavaPairDStream<String, Integer> wordcounter = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        wordcounter.print();

        wordcounter.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public void call(JavaPairRDD<String, Integer> wordcountsRDD)
                    throws Exception {
                wordcountsRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public void call(Iterator<Tuple2<String, Integer>> wordcounts) throws Exception {
                        HbaseDaoImpl dao = new HbaseDaoImpl();
                        String[] fy={"f1"};
                        dao.createTable("sparkWC",fy);
                        Tuple2<String, Integer> wordcount = null;
                        while (wordcounts.hasNext()) {
                            wordcount = wordcounts.next();
                            dao.insert("sparkWC", wordcount._1 + "_" + wordcount._2, "f1",
                                    new String[]{wordcount._1},
                                    new String[]{wordcount._2.toString()});
                        }
                    }
                });
            }

        });

        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }
}
