package com.atguigu.App.Function;
import com.alibaba.fastjson.JSONObject;

import java.text.ParseException;

// 维度关联接口
public interface DimJoinFunction<T> {

    public abstract String getKey(T obj);

    public abstract void join(T obj, JSONObject dimInfoObj) throws ParseException;
}
