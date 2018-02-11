package ls.spark.jiaotongshikuang

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Administrator on 2015/6/1.
  */
object PredictLRwithLBFGS {

    val sparkConf = new SparkConf().setAppName("Shanghai traffic").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)

    // create the date/time formatters
    val dayFormat = new SimpleDateFormat("yyyyMMdd")
    val minuteFormat = new SimpleDateFormat("HHmm")
    val sdf = new SimpleDateFormat( "yyyy-MM-dd_HH:mm:ss" );

    def main(args: Array[String]) {

        val input = "2016-03-31_09:50:00"
        val date = sdf.parse( input );
        val inputTimeLong = date.getTime()
        val inputTime = new Date(inputTimeLong)
        val day = dayFormat.format(inputTime)

        // fetch data from redis
        val jedis = RedisClient.pool.getResource
        jedis.select(1)

        // find relative road monitors for specified road
        // val camera_ids = List("310999003001","310999003102","310999000106","310999000205","310999007204")
        val camera_ids = List("310999003001","310999003102")
        val camera_relations:Map[String,Array[String]] = Map[String,Array[String]](
            "310999003001" -> Array("310999003001","310999003102","310999000106","310999000205","310999007204"),
            "310999003102" -> Array("310999003001","310999003102","310999000106","310999000205","310999007204")
        )

        val temp = camera_ids.map({ camera_id =>
            val list = camera_relations.get(camera_id).get

            val relations = list.map({ camera_id =>
                println(camera_id)
                // fetch records of one camera for three hours ago
                (camera_id, jedis.hgetAll(day + "_" + camera_id))

            })

            relations.foreach(println)

            // organize above records per minute to train data set format (MLUtils.loadLibSVMFile)
            val aaa = ArrayBuffer[Double]()
            // get current minute and recent two minutes
            for(index <- 0 to 2){

                val tempOne = inputTimeLong - 60 * 1000 * index
                val tempMinute = minuteFormat.format(inputTime)

                for((k,v) <- relations){
                    // k->camera_id ; v->speed
                    val map = v
                    if (map.containsKey(tempMinute)){
                        val info = map.get(tempMinute).split("_")
                        val f = info(0).toFloat / info(1).toFloat
                        aaa += f
                    } else{
                        aaa += -1.0
                    }
                }
            }

            // Run training algorithm to build the model
            val path = jedis.hget("model",camera_id)
            val model = LogisticRegressionModel.load(sc, path)

            // Compute raw scores on the test set.
            val prediction = model.predict(Vectors.dense(aaa.toArray))
            println(input+"\t"+camera_id+"\t"+prediction+"\t")
            jedis.hset(input, camera_id, prediction.toString)
        })

        RedisClient.pool.returnResource(jedis)
    }
 }
