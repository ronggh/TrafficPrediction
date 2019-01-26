package producer

import java.text.DecimalFormat
import java.util.Calendar

import com.alibaba.fastjson.JSON
import model.MonitorEvent
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import utils.PropertyUtils

import scala.util.Random

/**
  * 模拟产生车速数据，封装为JSON，发送给Kafka
  */
object Producer {

  def main(args: Array[String]): Unit = {
    // 读取Kafka配置信息
    val props = PropertyUtils.properties

    // 创建Kafka生产者对象
    val producer = new KafkaProducer[String, String](props)

    // 模拟产生实时车速，5分钟切换一次随机数范围
    var startTime = Calendar.getInstance().getTimeInMillis() / 1000
    val trafficCycle = 5 * 60

    val df = new DecimalFormat("0000")

    // 产生模拟数据
    while (true) {
      // 模拟产生监测点，1-20
      val monitorId = df.format(Random.nextInt(20) + 1)
      // 模拟车速
      var speed = "000"
      val currentTime = Calendar.getInstance().getTimeInMillis() / 1000
      // 是否到了要切换的时间
      if (currentTime - startTime > trafficCycle) {
        // 30以下
        speed = new DecimalFormat("000").format(Random.nextInt(30))
        // 进入到第二个5分钟，则重置startTime
        if (currentTime - startTime > 2 * trafficCycle) {
          startTime = currentTime
        }
      }
      else {
        // 31-60
        speed = new DecimalFormat("000").format(Random.nextInt(30) + 31)
      }

      //

      // 序列化封装成json,FastJson与scala的case class类不兼容，因为没有默认的构造函数
      val event = new MonitorEvent(monitorId, speed)
      val json = JSON.toJSON(event)
      println(json)

      // 发送到Kafka
      producer.send(new ProducerRecord[String, String](props.getProperty("kafka.topics"), json.toString))

      // 每200毫秒产生一条模拟数据
      Thread.sleep(200)
    }
  }
}
