package com.atguigu.Utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;
import redis.clients.jedis.Jedis;

import java.util.List;

// 查询维度
public class DimUtil {

    // 该工具类，通过给定 表名和字段信息    ====>   JSONObj, 同时存在 redis删除字段信息缓存的功能

    // 根据给定参数信息拼接为查询 sql，之后直接调用 phoenixUtil 查询结果
    public static JSONObject getDimInfoNoCache(String tableName, Tuple2<String,String>... colNameAndValue){
        String condition = new String(" where ");
        for (int i = 0; i < colNameAndValue.length; i++) {
            Tuple2<String, String> nameValueTuple = colNameAndValue[i];
            String fieldName = nameValueTuple.f0;
            String fieldValue = nameValueTuple.f1;
            if(i>0){
                condition += " and ";
            }
            condition += fieldName + "= " + "'"+ fieldValue + "'";
        }
        String sql = "select * from " + tableName + condition;
        System.out.println("sql to be executed: "+sql);

        JSONObject dimInfoJsonObj = null;
        List<JSONObject> dimList = PhoenixUtil.queryList(sql, JSONObject.class);
        if(dimList != null && dimList.size() > 0){
            dimInfoJsonObj = dimList.get(0);
        }else{
            System.out.println("维度数据未找到： "+ sql);
        }
        return dimInfoJsonObj;
    }


    // 先从 redis中查，如果没有，再通过 phoenix查询
    public static JSONObject getDimInfo(String tableName, String id){
        Tuple2<String, String> kv = Tuple2.of("id", id);
        return getDimInfo(tableName,kv);
    }

    // 根据给定参数信息拼接为查询 sql，
    public static JSONObject getDimInfo(String tableName, Tuple2<String,String>... colNameAndValue){
        String condition = " where ";
        String redisKey = tableName.toLowerCase() +":";
        for (int i = 0; i < colNameAndValue.length; i++) {
            Tuple2<String, String> nameValueTuple = colNameAndValue[i];
            String fieldName = nameValueTuple.f0;
            String fieldValue = nameValueTuple.f1;
            if(i>0){
                condition += " and ";
                // 把所有的值按顺序拼接
                redisKey += "_";
            }
            condition += fieldName + "=" + "'" + fieldValue +"'";
            redisKey += fieldValue;
        }

        Jedis jedis;
        String dimJsonStr;
        JSONObject dimJsonObj = null;

        jedis = RedisUtil.getJedis();
        dimJsonStr = jedis.get(redisKey);

        if(dimJsonStr != null && dimJsonStr.length()>0){
            dimJsonObj = JSON.parseObject(dimJsonStr);

        }else {
            // 如果没有在 redis查到结果，那么需要用 phoenix查询
            String sql = "select * from " + tableName + condition;
            List<JSONObject> dimList = PhoenixUtil.queryList(sql, JSONObject.class);

           // 对于维度数据，一般都是根据主键进行查询，不可能返回多条数据
            if(dimList!=null && dimList.size()>0){
                dimJsonObj = dimList.get(0);
                if(jedis != null){
                    jedis.setex(redisKey, 3600*24, dimJsonObj.toJSONString());
                }else {
                    System.out.println("维度数据没有找到： " + sql);
                }
            }
        }
        if(jedis!= null){
                jedis.close();
        }

        return dimJsonObj;
    }

    public static void deleteCached(String tableName, String id){
        String key = "dim:" + tableName.toLowerCase() + ":" + id;
        Jedis jedis = RedisUtil.getJedis();
        jedis.del(key);
        jedis.close();
    }



    public static void main(String[] args) {
//        JSONObject dimInfoNoCache = DimUtil.getDimInfoNoCache("dim_base_trademark", Tuple2.of("id", "11"));
//        JSONObject dimInfo = DimUtil.getDimInfo("dim_base_trademark", "11");
        JSONObject dimInfo = DimUtil.getDimInfo("dim_base_trademark", Tuple2.of("id", "11"));
        System.out.println(dimInfo);
    }
}
