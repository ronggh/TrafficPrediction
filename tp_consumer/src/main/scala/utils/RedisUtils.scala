package utils

import redis.clients.jedis.{JedisPool, JedisPoolConfig}

object RedisUtils {
  // 配置Redis连接信息
  val host="192.168.154.101"
  val port = 6379
  val timeout = 30000

  val config = new JedisPoolConfig

  // 设置最大连接数
  config.setMaxTotal(200)
  // 设置最大空闲连接数
  config.setMaxIdle(50)
  //设置最小空闲数
  config.setMinIdle(8)

  // 连接时的最大等待毫秒数
  config.setMaxWaitMillis(10000)
  // 设置在获取连接时，是否检查连接的有效性
  config.setTestOnBorrow(true)
  // 设置释放连接到连接池时是否检验连接的有效性
  config.setTestOnReturn(true)
  //idle 时进行连接扫描
  config.setTestWhileIdle(true)
  //表示idle object evitor两次扫描之间要sleep的毫秒数
  config.setTimeBetweenEvictionRunsMillis(30000)
  //表示idle object evitor每次扫描的最多的对象数
  config.setNumTestsPerEvictionRun(10)
  //表示一个对象至少停留在idle状态的最短时间，然后才能被idle object evitor扫描并驱逐；这一项只有在timeBetweenEvictionRunsMillis大于0时才有意义
  config.setMinEvictableIdleTimeMillis(60000)
  //连接池
  lazy val jedisPool  = new JedisPool(config, host, port, timeout)

  //释放资源
  lazy val hook = new Thread {
    override def run() = {
      jedisPool.destroy()
    }
  }
  sys.addShutdownHook(hook.run)

  }
