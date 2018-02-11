package ls.spark.learn.other.scala

/**
  * Created by FromX on 2017/3/20.
  */

object Test {
  def main(args: Array[String]) {
    val pattern = "Scala".r
    val str = "Scala is Scalable and cool Scala"

    println(pattern findAllIn   str)
  }
}