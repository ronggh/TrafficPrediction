package utils

import java.util.Properties

object PropertyUtils {

  val properties = new Properties()

  try{
    //加载配置属性
    val inputStream = ClassLoader.getSystemResourceAsStream("kafka.properties")
    //println(inputStream)
    properties.load(inputStream)
    //println(properties)
    //println(properties.getProperty("bootstrap.servers"))
  }catch {
    case ex:Exception => println(ex)
  }finally {}

  // 通过key得到kafka的属性值
  def getProperty(key: String): String = properties.getProperty(key)

}
