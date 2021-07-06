package com.atguigu.App.practice;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.App.Function.DimSink;
import com.atguigu.App.Function.TableProcessFunction;
import com.atguigu.Utils.MyKafkaUtil;
import com.atguigu.bean.TableProcess;
import com.google.inject.internal.util.$Nullable;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class BaseDBApp1 {
    public static void main(String[] args) throws Exception {

        System.setProperty("HADOOP_USER_NAME", "atguigu");
        String topic = "ods_base_db_m";
        String groupId = "ods_base_group";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10000);
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop201:9820/gmall/flink/checkpoint"));

        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        DataStreamSource<String> sourceDS = env.addSource(kafkaSource);

        SingleOutputStreamOperator<JSONObject> jsonObjDS = sourceDS.map(jsonStr -> JSON.parseObject(jsonStr));
        SingleOutputStreamOperator<JSONObject> filteredDS = jsonObjDS.filter(
                jsonObj -> jsonObj.getJSONObject("data") != null && jsonObj.getString("data").length()>3 && jsonObj.getString("table")!=null
        );

        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>(TableProcess.SINK_TYPE_HBASE){};
        SingleOutputStreamOperator<JSONObject> splitDS = filteredDS.process(new tableprocessfun(hbaseTag));

        DataStream<JSONObject> hbaseDS = splitDS.getSideOutput(hbaseTag);
        hbaseDS.print("hbase: >>>>>");
        hbaseDS.addSink(new DimSink1());

        splitDS.print("fact: >>>>>>");
        splitDS.addSink(MyKafkaUtil.getKafkaSinkBySchema(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObj, @Nullable Long timestamp) {
                String sink_table = jsonObj.getString("sink_table");
                JSONObject data = jsonObj.getJSONObject("data");
                return new ProducerRecord<>(sink_table, data.toJSONString().getBytes());
            }
        }));


        env.execute();
    }
}
