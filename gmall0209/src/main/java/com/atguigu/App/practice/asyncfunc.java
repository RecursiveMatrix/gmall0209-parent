package com.atguigu.App.practice;


import com.alibaba.fastjson.JSONObject;
import com.atguigu.Utils.DimUtil;
import com.google.inject.internal.util.$Join;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;


public abstract class asyncfunc<T> extends RichAsyncFunction<T,T> implements dimfun<T>{

    String tableName;
    ExecutorService executorService;
    public asyncfunc(String tableName){this.tableName = tableName;}


    @Override
    public void open(Configuration parameters) throws Exception {
        executorService = threadtoolutils1.getInstance();
    }

    @Override
    public void asyncInvoke(T orderwide, ResultFuture<T> resultFuture) throws Exception {
        executorService.submit(
                new Runnable() {
                    @Override
                    public void run() {
                        String key = getkey(orderwide);
                        JSONObject dimInfo = DimUtil.getDimInfo(tableName,key);
                        if (dimInfo!=null){
//                            join(dimInfo,orderwide);
                        }
                        resultFuture.complete(Arrays.asList(orderwide));
                    }
                }
        );
    }
}
