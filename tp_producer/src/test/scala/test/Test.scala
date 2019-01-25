package test

import utils.PropertyUtils

object Test {
  def main(args: Array[String]): Unit = {
    val props = PropertyUtils.properties
    val value = props.getProperty("bootstrap.servers")
    print("value:" + value)
  }
}
