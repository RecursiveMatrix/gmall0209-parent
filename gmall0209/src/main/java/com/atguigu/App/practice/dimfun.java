package com.atguigu.App.practice;

import com.alibaba.fastjson.JSONObject;

import java.text.ParseException;

public interface dimfun<T> {
    public abstract String getkey(T obj);
    public abstract void join(JSONObject dimObj, T obj) throws ParseException;
}
