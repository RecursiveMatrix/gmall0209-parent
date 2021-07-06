package com.atguigu.App.practice;

import com.alibaba.fastjson.JSONObject;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class test {
    public static void main(String[] args) throws ParseException {
//        String birthday = "1981-02-19";
//        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
//        Date res = sdf.parse(birthday);
//        Long res1 = res.getTime();
//        Long res2 = System.currentTimeMillis() - res1;
//        Long res3  = res2/365L/24L/3600L/1000L;
//        int res4 = res3.intValue();
//        System.out.println(res4);

        JSONObject result = new JSONObject();
        Stat1 stat1 = new Stat1();
        String jsonstr = "{\n" +
                "  \"status\": 0,\n" +
                "  \"msg\": \"\",\n" +
                "  \"data\": {\n" +
                "    \"categories\": [\n"  +
                "    ],\n" +
                "    \"series\": [\n" +
                "      {\n" +
                "        \"name\": \"上周\",\n" +
                "        \"data\": [\n"  +
                "        ]\n" +
                "      },\n" +
                "      {\n" +
                "        \"name\": \"昨日\",\n" +
                "        \"data\": [\n" +
                "        ]\n" +
                "      },\n" +
                "      {\n" +
                "        \"name\": \"今日\",\n" +
                "        \"data\": [\n" +
                "        ]\n" +
                "      }\n" +
                "    ]\n" +
                "  }\n" +
                "}";

        JSONObject res = JSONObject.parseObject(jsonstr);
        System.out.println(res);
    }
}
