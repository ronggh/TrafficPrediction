package consumer

import java.text.SimpleDateFormat
import java.util.Calendar

import com.alibaba.fastjson.{JSON, TypeReference}
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import utils.{PropertyUtils, RedisUtils}

object SparkConsumer {
  def main(args: Array[String]): Unit = {
    // 初始化Spark
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkConsumer")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(5))
    ssc.checkpoint("./ssc/checkpoint")

    // 配置Kafka
    val kafkaParams = Map("metadata.broker.list" -> PropertyUtils.getProperty("metadata.broker.list"))
    val topics = Set(PropertyUtils.getProperty("kafka.topics"))
    // 从Kafka中读数据,只需要从值中取，数据形式如：{"monitorId":"0015","speed":"035"}
    val kafkaLineDStream = KafkaUtils.createDirectStream[
      String,
      String,
      StringDecoder,
      StringDecoder](ssc, kafkaParams, topics)
      .map(_._2)

    // 解析json字符串
    //使用fastjson解析基于事件的每一条数据到Java的HashMap中
    val event = kafkaLineDStream.map(line => {
      val lineJavaMap = JSON.parseObject(line, new TypeReference[java.util.Map[String, String]](){})
      //将Java的HashMap转为Scala的mutable.Map
      import scala.collection.JavaConverters._
      val lineScalaMap: collection.mutable.Map[String, String] = mapAsScalaMapConverter(lineJavaMap).asScala
      println(lineScalaMap)
      lineScalaMap
    })

    //将每一条数据按照monitor_id聚合，聚合时每一条数据中的“车辆速度”叠加，“车辆个数”叠加
    //(monitor_id, (speed, 1))例如：(0005, (80,1))
    val sumOfSpeedAndCount = event
      .map(e => (e.get("monitorId").get, e.get("speed").get))  // ("0005","80")
      .mapValues(s => (s.toInt, 1))                            // ("0005",(80,1))
      //timeWindow为20秒，slideTime为10秒，严格意义应该设置为：60, 60,
      // 即，每隔60秒，统计60秒内的数据，因为我们打算按照分钟来采集数据
      // 对形如("0005",(80,1))和("0005",(58,1))的一系列数据进行聚合
      // monitorId（0005）作为key用来聚合。t1,t2是两个Tuple，形如(80,1)，（58，1）
      // 聚合合数据("0005",(138,2)),即车速和数量分别叠加
      .reduceByKeyAndWindow((t1: (Int, Int), t2: (Int, Int)) => (t1._1 + t2._1, t1._2 + t2._2),
      Seconds(20),
      Seconds(10))

    //数据库索引号
    val dbIndex = 0
    //将采集到的数据，按照每分钟采集到redis中，将用于后边的建模与分析
    sumOfSpeedAndCount.foreachRDD(rdd => {
      rdd.foreachPartition(partitionRecords => {
        //tuple: (monitor_id, (speed, 1))
        // 过滤一下数据，车辆数大于0的数据
        partitionRecords.filter((tuple: (String, (Int, Int))) => tuple._2._2 > 0).foreach(pair => {
          //开始取出这个60秒的window中的所有聚合后的数据
          //1872_15
          val jedis = RedisUtils.jedisPool.getResource
          val monitorId = pair._1
          val sumOfSpeed = pair._2._1
          val sumOfCarCount = pair._2._2

          //模拟数据为实时流入，则获取当前分钟作为车辆经过的分钟
          val currentTime = Calendar.getInstance().getTime
          val hourMinuteSDF = new SimpleDateFormat("HHmm")
          val dateSDF = new SimpleDateFormat("yyyyMMdd")

          val hourMinuteTime = hourMinuteSDF.format(currentTime)
          val date = dateSDF.format(currentTime)

          //选择存入的数据库
          jedis.select(dbIndex)
          jedis.hset(date + "_" + monitorId, hourMinuteTime, sumOfSpeed + "_" + sumOfCarCount)
          println(date + "_" + monitorId)
          RedisUtils.jedisPool.returnResource(jedis)
        })
      })
    })
    //让spark开始工作
    ssc.start()
    ssc.awaitTermination()
  }
}
