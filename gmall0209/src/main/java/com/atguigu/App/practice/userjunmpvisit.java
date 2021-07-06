package com.atguigu.App.practice;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;


public class userjunmpvisit {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String sourceTopic = "dwd_page_log";
        String groupId = "userJumpDetailApp";
        String sinkTopic = "dwm_user_jump_detail";

        // test
        DataStream<String> dataStream = env
                .fromElements(
                        "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000} ",
                        "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"house\"},\"ts\":19999} ",
                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"home\"},\"ts\":12000}",
                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"home\"},\"ts\":23000}",
                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
                                "\"home\"},\"ts\":4" +
                                "2000} ",
                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
                                "\"detail\"},\"ts\":34000} "
                );
        dataStream.print("in json:");

        SingleOutputStreamOperator<JSONObject> jsonObjDS = dataStream.map(jsonStr -> JSON.parseObject(jsonStr));
        SingleOutputStreamOperator<JSONObject> jsonObjwithwatermark = jsonObjDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject jsonObj, long recordTimestamp) {
                        return jsonObj.getLong("ts");
                    }
                }));

        KeyedStream<JSONObject, String> keyedDS = jsonObjwithwatermark.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("GoIn").where(
                new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObj) throws Exception {
                        String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                        return lastPageId == null || lastPageId.length() == 0;
                    }
                }
        ).next("next").where(
                new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObj) throws Exception {
                        String pageId = jsonObj.getJSONObject("page").getString("page_id");
                        return pageId != null && pageId.length() > 0;
                    }
                }
        ).within(Time.milliseconds(10000));

        PatternStream<JSONObject> patternDS = CEP.pattern(keyedDS, pattern);
        OutputTag<JSONObject> timeoutTag = new OutputTag<JSONObject>("timeout"){};

        SingleOutputStreamOperator<JSONObject> filteredDS = patternDS.flatSelect(
                timeoutTag,
                new PatternFlatTimeoutFunction<JSONObject, JSONObject>() {
                    @Override
                    public void timeout(Map<String, List<JSONObject>> pattern, long timeoutTimestamp, Collector<JSONObject> out) throws Exception {
                        List<JSONObject> results = pattern.get("GoIn");
                        for (JSONObject result : results) {
                            out.collect(result);
                        }
                    }
                },
                new PatternFlatSelectFunction<JSONObject, JSONObject>() {
                    @Override
                    public void flatSelect(Map<String, List<JSONObject>> pattern, Collector<JSONObject> out) throws Exception {

                    }
                }
        );
        DataStream<JSONObject> sideOutput = filteredDS.getSideOutput(timeoutTag);
        sideOutput.print("超时数据： >>>>> ");


        env.execute();

    }

}
