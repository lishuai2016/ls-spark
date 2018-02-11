package ls.spark.learn.other.sxt

import org.apache.spark.{SparkContext, SparkConf}

object MySparkPi {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Pi").setMaster("local[2]")
    val spark = new SparkContext("local[2]","Spark Pi");

    val slices = 100;
    val n = 1000 * slices
    val count = spark.parallelize(1 to n,slices).map({ i =>
      /** Returns a `double` value with a positive sign, greater than or equal
        *  to `0.0` and less than `1.0`.
        */
      def random: Double = java.lang.Math.random()
      val x = random * 2 - 1
      val y = random * 2 - 1
      println(x+"--"+y)
      if (x*x + y*y < 1) 1 else 0

    }).reduce(_ + _)

    println("Pi is roughly " + 4.0 * count / n)


    spark.stop()
  }
}
