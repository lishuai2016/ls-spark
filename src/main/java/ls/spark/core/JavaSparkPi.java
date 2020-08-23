package ls.spark.core;

/**
spark-submit --num-executors 2 --driver-cores 1 --driver-memory 512M --executor-cores 2 --executor-memory 512M --class org.apache.spark.examples.JavaSparkPi SparkDemo-1.0-SNAPSHOT.jar

 */

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.List;

/**
 * 计算π的近似值
 * <p>
 * <b>用法：</b> JavaSparkPi [slices] <br>
 * <b>原理：</b> 使用蒙特卡罗方法计算圆周率，假想有一个 2 * 2 的正方形，在里面画一个内切圆(r = 1)，
 * 假想有一个点随机扔到正方形中(假设有 N 次)，那么恰好也在圆中的次数为(C)，如果 N 足够大，那么 C/N 逼近于 圆的面积/正方形面积，也就是说
 * pi/4，那么 pi/4 = C/N, pi = 4*C/N.
 */
public final class JavaSparkPi {

    public static void main(String[] args) throws Exception {
        //输出接收到的参数
        if (args != null && args.length > 0 ) {
            for (int i = 0; i < args.length; i++) {
                System.out.println(String.format("第%s个参数为：%s",i,args[i]));
            }
        } else {
            System.out.println("没有参数输入");
        }
        SparkConf sparkConf = new SparkConf().setAppName("JavaSparkPi");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        // slices 对应于 partition 个数
        int slices = (args.length == 1) ? Integer.parseInt(args[0]) : 2;
        int n = 100000 * slices; // 每个 partition 中设置 100000 个点
        List<Integer> l = new ArrayList<Integer>(n);
        for (int i = 0; i < n; i++) {
            l.add(i);
        }

        JavaRDD<Integer> dataSet = jsc.parallelize(l, slices);

        int count = dataSet.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer integer) {
                double x = Math.random() * 2 - 1;
                double y = Math.random() * 2 - 1;
                return (x * x + y * y < 1) ? 1 : 0; // 在圆内返回 1，否则返回 0
            }
        }).reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) {
                return integer + integer2;
            }
        }); // count表示在圆内的点的数目

        System.out.println("marlTest-->>SparkDemo-->>JavaSparkPi-->>Pi is roughly " + 4.0 * count / n);

        jsc.close();
    }
}