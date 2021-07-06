package com.atguigu.App.Function;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.Utils.DimUtil;
import com.atguigu.Utils.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;

// 维度关联，使用泛型模板 T表示异步通讯流的数据类型，继承接口为获取 key和 join方法，因为每个维度表的主键和关联方式不同，需要调用时再指定
// getkey方法：从数据流中获取key的方法(使用时指定)，通过这个key和维度表的名字获取表中的维度信息：dimUtil
// 如果有的话，使用 join方法将数据流中的 对象和维度表对象关联起来，最后使用resultfuture向下传递

public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T,T> implements DimJoinFunction<T>{

    /*
    发送异步请求：
    输入为事实数据，从线程池中获取线程
    输出为异步请求处理后的返回结果
     */

    private ExecutorService executorService; // 线程池对象的父接口，相当于多态
    private String tableName;
    public DimAsyncFunction(String tableName){
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        executorService = ThreadPoolUtil.getInstance();
    }



    @Override
    public void asyncInvoke(T obj, ResultFuture<T> resultFuture) throws Exception {
        executorService.submit(
                new Runnable() {
                    @Override
                    public void run() {
                       try{
                           // 发送异步请求：先获取维度数据，根据维度的主键到维度表中查询，记录系统时间
                           long start = System.currentTimeMillis();
                           // 通过构造方法传入表名
                           // 从事实数据流中获取 key
                           String key = getKey(obj);
                           JSONObject dimInfoObj = DimUtil.getDimInfo(tableName, key);
                           System.out.println("维度数据JSON格式： "+ dimInfoObj);
                           if (dimInfoObj != null){
                               // 维度关联: 流中的事实数据和维度数据关联
                               join(obj,dimInfoObj);
                           }
                           System.out.println("维度关联后的对象： "+ obj);
                           long end = System.currentTimeMillis();
                           System.out.println("异步查询耗时： " + (end-start)+" 毫秒");
                           // 将关联后的数据向下传递
                           resultFuture.complete(Arrays.asList(obj));
                       }catch (Exception e){
                           e.printStackTrace();
                           throw new RuntimeException(tableName + " 维度查询失败");
                       }
                    }
                }
        );
    }
}
