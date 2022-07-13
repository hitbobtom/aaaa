package com.atguigu.jedis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.ListPosition;

/**
 * Jedis
 */
public class JedisDemo {


    public static void main(String[] args) {

        //Jedis jedis = getJedis();
        Jedis jedis = getJedisFormPool();
        String result = jedis.ping();
        System.out.println(result);
        jedis.close();
    }


    public static JedisPool pool = null ;

    /**
     * 作业: 测试每种类型对应的API方法
     */
    public static void testString(){
        Jedis jedis = getJedisFormPool();
        //jedis.set()
        //jedis.get()
        //.....

        jedis.linsert("a" , ListPosition.BEFORE, "" , "");
        jedis.close();
    }

    public static void testList(){}

    public static void testSet(){}

    public static void testZset(){}

    public static void testHash(){}


    /**
     * 基于连接池获取连接
     * @return
     */
    public static Jedis  getJedisFormPool(){
        if(pool == null ){
            //创建好连接池
            //主要配置
            JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
            jedisPoolConfig.setMaxTotal(10); //最大可用连接数
            jedisPoolConfig.setMaxIdle(5); //最大闲置连接数
            jedisPoolConfig.setMinIdle(5); //最小闲置连接数
            jedisPoolConfig.setBlockWhenExhausted(true); //连接耗尽是否等待
            jedisPoolConfig.setMaxWaitMillis(2000); //等待时间
            jedisPoolConfig.setTestOnBorrow(true); //取连接的时候进行一下测试 ping pong

            String host = "hadoop102" ;
            int port = 6379 ;
            pool= new JedisPool(jedisPoolConfig , host , port );
        }

        //直接获取连接
        Jedis jedis = pool.getResource();
        return jedis ;
    }



    public static Jedis getJedis(){
        String host = "hadoop102";
        int port = 6379 ;
        Jedis jedis = new Jedis(host, port);
        return jedis;
    }
}
