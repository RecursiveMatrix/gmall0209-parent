package com.atguigu.App.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.Utils.ClickhouseUtil;
import com.atguigu.Utils.MyKafkaUtil;
import com.atguigu.bean.VisitorStats;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

public class VisitorStatsApp {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        String groupId = "visitor_stats_app";

        //TODO 1.从Kafka的pv、uv、跳转明细主题中获取数据
        String pageViewSourceTopic = "dwd_page_log";
        String uniqueVisitSourceTopic = "dwm_unique_visit";
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";

        FlinkKafkaConsumer<String> pageSource = MyKafkaUtil.getKafkaSource(pageViewSourceTopic, groupId);
        FlinkKafkaConsumer<String> uvSource = MyKafkaUtil.getKafkaSource(uniqueVisitSourceTopic, groupId);
        FlinkKafkaConsumer<String> ujSource = MyKafkaUtil.getKafkaSource(userJumpDetailSourceTopic, groupId);
        DataStreamSource<String> pageDS = env.addSource(pageSource);
        DataStreamSource<String> uvDS = env.addSource(uvSource);
        DataStreamSource<String> ujDS = env.addSource(ujSource);

        SingleOutputStreamOperator<VisitorStats> pageObj = pageDS.map(
                str -> {
                    JSONObject jsonObj = JSON.parseObject(str);
                    return new VisitorStats("", "",
                            jsonObj.getJSONObject("common").getString("vc"),
                            jsonObj.getJSONObject("common").getString("ch"),
                            jsonObj.getJSONObject("common").getString("ar"),
                            jsonObj.getJSONObject("common").getString("is_new"),
                            0L, 1L, 0L, 0L,
                            jsonObj.getJSONObject("page").getLong("during_time"),
                            jsonObj.getLong("ts"));
                }
        );

        SingleOutputStreamOperator<VisitorStats> uvObj = uvDS.map(
                str -> {
                    JSONObject jsonObj = JSON.parseObject(str);
                    return new VisitorStats("", "",
                            jsonObj.getJSONObject("common").getString("vc"),
                            jsonObj.getJSONObject("common").getString("ch"),
                            jsonObj.getJSONObject("common").getString("ar"),
                            jsonObj.getJSONObject("common").getString("is_new"),
                            1L, 0L, 0L, 0L, 0L,
                            jsonObj.getLong("ts"));
                }
        );

        SingleOutputStreamOperator<VisitorStats> svObj = pageDS.process(
                new ProcessFunction<String, VisitorStats>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<VisitorStats> out) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(value);
                        String lastPage = jsonObj.getJSONObject("page").getString("last_page_id");
                        if (lastPage == null || lastPage.length() == 0) {
                            VisitorStats visitorStats = new VisitorStats("", "",
                                    jsonObj.getJSONObject("common").getString("vc"),
                                    jsonObj.getJSONObject("common").getString("ch"),
                                    jsonObj.getJSONObject("common").getString("ar"),
                                    jsonObj.getJSONObject("common").getString("is_new"),
                                    0L, 0L, 1L, 0L, 0L,
                                    jsonObj.getLong("ts"));
                            out.collect(visitorStats);
                        }
                    }
                }
        );


        SingleOutputStreamOperator<VisitorStats> ujObj = ujDS.map(
                str -> {
                    JSONObject jsonObj = JSON.parseObject(str);
                    return new VisitorStats("", "",
                            jsonObj.getJSONObject("common").getString("vc"),
                            jsonObj.getJSONObject("common").getString("ch"),
                            jsonObj.getJSONObject("common").getString("ar"),
                            jsonObj.getJSONObject("common").getString("is_new"),
                            0L, 0L, 0L, 1L, 0L,
                            jsonObj.getLong("ts"));
                }
        );

        DataStream<VisitorStats> unionDS = pageObj.union(uvObj, svObj, ujObj);
        SingleOutputStreamOperator<VisitorStats> unionDSwithWM = unionDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<VisitorStats>() {
                                    @Override
                                    public long extractTimestamp(VisitorStats element, long recordTimestamp) {
                                        return element.getTs();
                                    }
                                }
                        )
        );

        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> keyedDS = unionDSwithWM.keyBy(
                new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
                    @Override
                    public Tuple4<String, String, String, String> getKey(VisitorStats value) throws Exception {
                        return new Tuple4<>(value.getVc(), value.getCh(), value.getAr(), value.getIs_new());
                    }
                }
        );

        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> windowDS = keyedDS.window(TumblingEventTimeWindows.of(Time.seconds(10)));

        SingleOutputStreamOperator<VisitorStats> finalDS = windowDS.reduce(
                new ReduceFunction<VisitorStats>() {
                    @Override
                    public VisitorStats reduce(VisitorStats value1, VisitorStats value2) throws Exception {
                        value1.setUv_ct(value1.getUv_ct() + value2.getUv_ct());
                        value1.setSv_ct(value1.getSv_ct() + value2.getSv_ct());
                        value1.setUj_ct(value1.getUj_ct() + value2.getUj_ct());
                        value1.setPv_ct(value1.getPv_ct() + value2.getPv_ct());
                        value1.setDur_sum(value1.getDur_sum() + value2.getDur_sum());
                        return value1;
                    }
                },
                new ProcessWindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
                    @Override
                    public void process(Tuple4<String, String, String, String> stringStringStringStringTuple4, Context context, Iterable<VisitorStats> elements, Collector<VisitorStats> out) throws Exception {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        for (VisitorStats element : elements) {
                            String start = sdf.format(new Date(context.window().getStart()));
                            String end = sdf.format(new Date(context.window().getEnd()));
                            element.setTs(new Date().getTime());
                            element.setStt(start);
                            element.setEdt(end);
                            out.collect(element);
                        }
                    }
                }
        );

        finalDS.print("result : >>>>> ");
        /*
            finalDS.addSink(ClickhouseUtil.getJdbcSink("insert into visitor_stats_0820 values()"));

         */
        finalDS.addSink(ClickhouseUtil.getJdbcSink(
                "insert into visitor_stats_0820 values(?,?,?,?,?,?,?,?,?,?,?,?)")
        );

        env.execute();
    }
}
