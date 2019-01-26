package prediction

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors
import utils.RedisUtils

import scala.collection.mutable.ArrayBuffer


/**
  * 堵车预测：根据训练出来的模型进行堵车预测
  */
object Prediction {
  def main(args: Array[String]): Unit = {
    //配置spark
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Prediction")
    val sc = new SparkContext(sparkConf)

    //想要预测的监测点
    val dateSDF = new SimpleDateFormat("yyyyMMdd")
    val hourMinuteSDF = new SimpleDateFormat("HHmm")
    val userSDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    // 预测时间
    val inputDateString = "2019-1-26 21:00:00"
    val inputDate = userSDF.parse(inputDateString)

    val dayOfInputDate = dateSDF.format(inputDate)
    val hourMinuteOfInputDate = hourMinuteSDF.format(inputDate)

    val monitorIDs = List("0005", "0015")
    val monitorRelations = Map[String, Array[String]](
      "0005" -> Array("0003", "0004", "0005", "0006", "0007"),
      "0015" -> Array("0013", "0014", "0015", "0016", "0017"))

    //从redis中读取数据
    //redis的数据库索引
    val dbIndex = 0
    //获取redis中的实时数据
    val jedis = RedisUtils.jedisPool.getResource
    jedis.select(dbIndex)
    val temp = monitorIDs.map(monitorID =>
    {
      val monitorRelationList = monitorRelations.get(monitorID).get
      val relationsInfo = monitorRelationList.map(monitorID =>
        //
        (monitorID, jedis.hgetAll(dayOfInputDate + "_" + monitorID)))
      //数据准备
      val dataX = ArrayBuffer[Double]()
      // 装载目标时间点3分钟前的数据
      for(index <- Range(3, 0, -1)){
        val oneMoment = inputDate.getTime - 60 * index * 1000
        val oneHM = hourMinuteSDF.format(new Date(oneMoment))

        for((k, v) <- relationsInfo){
          if(v.containsKey(oneHM)){
            val speedAndCarCount = v.get(oneHM).split("_")
            val valueX = speedAndCarCount(0).toFloat / speedAndCarCount(1).toFloat
            dataX += valueX
          }else{
            dataX += 60.0F
          }
        }
      }
      println(Vectors.dense(dataX.toArray))
      //加载模型
      val modelPath = jedis.hget("model", monitorID)
      val model = LogisticRegressionModel.load(sc, modelPath)

      //预测
      val prediction = model.predict(Vectors.dense(dataX.toArray))
      println(monitorID + ",堵车评估值：" + prediction + "，是否通畅：" + (if(prediction >= 4) "通畅" else "不通畅"))

      //结果保存
      jedis.hset(inputDateString, monitorID, prediction.toString)

    })

    RedisUtils.jedisPool.returnBrokenResource(jedis)


  }

}
