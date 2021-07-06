package com.atguigu.Utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

// 获取 redis连接
public class RedisUtil {

    public static JedisPool jedisPool = null;
    public static Jedis getJedis(){
        if(jedisPool == null){
            JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
            jedisPoolConfig.setMaxTotal(100);
            jedisPoolConfig.setBlockWhenExhausted(true);
            jedisPoolConfig.setMaxWaitMillis(3000);
            jedisPoolConfig.setMaxIdle(5);
            jedisPoolConfig.setMinIdle(5);
            jedisPoolConfig.setTestOnBorrow(true);

            jedisPool = new JedisPool(jedisPoolConfig, "hadoop201", 6379, 1000);
            System.out.println("开辟连接池 ...");
            return jedisPool.getResource();
        }else {
            System.out.println(" 连接池： "+ jedisPool.getNumActive());
            return jedisPool.getResource();
        }
    }
}
