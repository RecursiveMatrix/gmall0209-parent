package com.atguigu.App.practice;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.Utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.text.SimpleDateFormat;
import java.util.Date;

public class uniquevisitapp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        String groupId = "unique_visit_app";
        String sourceTopic = "dwd_page_log";
        String sinkTopic = "dwm_unique_visit";

        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(sourceTopic, groupId);
        DataStreamSource<String> jsonStrDS = env.addSource(kafkaSource);
        SingleOutputStreamOperator<JSONObject> jsonObjDS = jsonStrDS.map(jsonStr -> JSON.parseObject(jsonStr));
        KeyedStream<JSONObject, String> keyedJsonObjDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        SingleOutputStreamOperator<JSONObject> filteredDS = keyedJsonObjDS.filter(
                new RichFilterFunction<JSONObject>() {
                    ValueState<String> lastVisitDateState = null;
                    SimpleDateFormat sdf;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        sdf = new SimpleDateFormat("yyyy-MM-dd");
                        if (lastVisitDateState == null) {
                            ValueStateDescriptor<String> lastVisitDateDescriptor = new ValueStateDescriptor<>("lastViewDate", String.class);
                            StateTtlConfig conf = StateTtlConfig.newBuilder(Time.days(1)).build();
                            lastVisitDateDescriptor.enableTimeToLive(conf);
                            lastVisitDateState = getRuntimeContext().getState(lastVisitDateDescriptor);
                        }
                    }

                    @Override
                    public boolean filter(JSONObject jsonObj) throws Exception {
                        String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                        Long ts = jsonObj.getLong("ts");

                        // TODO bug:
                        String currentDate = sdf.format(ts);
                        String recordDate = lastVisitDateState.value();
                        if (lastPageId != null && lastPageId.length() > 0) {
                            return false;
                        }
                        if (recordDate != null && recordDate.length() > 0 && recordDate.equals(currentDate)) {
                            return false;
                        } else {
                            lastVisitDateState.update(currentDate);
                            return true;
                        }
                    }
                }
        );

        filteredDS.print("filtered: >>>>");
        SingleOutputStreamOperator<String> finalDS = filteredDS.map(jsonObj -> jsonObj.toString());
        FlinkKafkaProducer<String> kafkaSink = MyKafkaUtil.getKafkaSink(sinkTopic);
        finalDS.addSink(kafkaSink);


        env.execute();
    }
}
