import utils.RedisUtils

object TestJedisUtils {
  def main(args: Array[String]): Unit = {
    val jedis = RedisUtils.jedisPool.getResource
    jedis.select(1)
    jedis.set("0001","5670_30")
    RedisUtils.jedisPool.returnResource(jedis)
  }

}
