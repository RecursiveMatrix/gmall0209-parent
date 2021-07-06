package com.atguigu.App.dwm;

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

// 从指定dwd获取数据，计算每日独立访问，即去重
public class UniqueVisitApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        String groupId = "unique_visit_app";
        String sourceTopic = "dwd_page_log";
        String sinkTopic = "dwm_unique_visit";

        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(sourceTopic, groupId);
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);

        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(jsonStr -> JSON.parseObject(jsonStr));
//        jsonObjDS.print("jsonObjDs >>>>>");
        KeyedStream<JSONObject, String> keyedJsonObj = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        SingleOutputStreamOperator<JSONObject> filteredDS = keyedJsonObj.filter(new RichFilterFunction<JSONObject>() {

            ValueState<String> lastVisitDate = null;
            SimpleDateFormat sdf;

            @Override
            public void open(Configuration parameters) throws Exception {
                sdf = new SimpleDateFormat("yyyy-MM-dd");
                // 初始化状态，定义有效时间
                if (lastVisitDate == null) {
                    // 如果状态为空，先定义描述器，再配置状态，调用配置器的方法，使用该配置，最后获取状态
                    ValueStateDescriptor<String> lastVisitDateDescriptor = new ValueStateDescriptor<>("lastViewDate", String.class);
                    StateTtlConfig stateValidConf = StateTtlConfig.newBuilder(Time.days(1)).build();
                    lastVisitDateDescriptor.enableTimeToLive(stateValidConf);
                    lastVisitDate = getRuntimeContext().getState(lastVisitDateDescriptor);
                }
            }

            @Override
            public boolean filter(JSONObject jsonObj) throws Exception {
                String lastPageID = jsonObj.getJSONObject("page").getString("last_page_id");
                Long ts = jsonObj.getLong("ts");

                // TODO : bug 因为日期格式带 -- ，不需要再用date包装
                String pageDate = sdf.format(ts);
                String lastViewDate = this.lastVisitDate.value();
                if (lastPageID != null && lastPageID.length() > 0) {
                    return false;
                }
                if (lastViewDate != null && lastViewDate.length() > 0 && lastViewDate.equals(pageDate)) {
                    return false;
                } else {
                    lastVisitDate.update(pageDate);
                    return true;
                }
            }
        });

        filteredDS.print("uv >>>>> ");

        SingleOutputStreamOperator<String> jsonStrDS = filteredDS.map(jsonObj -> jsonObj.toString());
        FlinkKafkaProducer<String> kafkaSink = MyKafkaUtil.getKafkaSink(sinkTopic);
        jsonStrDS.addSink(kafkaSink);
        env.execute();

    }
}
