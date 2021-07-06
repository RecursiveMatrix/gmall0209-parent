package com.atguigu.App.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.App.Function.DimSink;
import com.atguigu.App.Function.TableProcessFunction;
import com.atguigu.Utils.MyKafkaUtil;
import com.atguigu.bean.TableProcess;
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


public class BaseDBApp {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60000);
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop201:9820/gmall/flink/checkpoint"));
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        String topic = "ods_base_db_m";
        String groupId = "ods_base_group";

        // TODO 0. 通过 Maxwell监控 MySQL中的数据变化，把变化记录到 kafka

        FlinkKafkaConsumer<String> kafkaDBSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        DataStreamSource<String> DBDS = env.addSource(kafkaDBSource);

        SingleOutputStreamOperator<JSONObject> jsonObjDS = DBDS.map(jsonStr -> JSON.parseObject(jsonStr));
        jsonObjDS.print();

        // TODO 1. 初步清洗数据
        SingleOutputStreamOperator<JSONObject> filteredDS = jsonObjDS.filter(
                jsonObject -> {
                    boolean flag = jsonObject.getString("table") != null
                            && jsonObject.getJSONObject("data") != null
                            && jsonObject.getString("data").length() > 3;
                    return flag;
                }
        );

        // TODO 2. 核心 process 处理方法，对于每个 json对象，根据具体情况，发送到不同的位置，提前定义发往 hbase侧输出流的标签
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>(TableProcess.SINK_TYPE_HBASE){};

        SingleOutputStreamOperator<JSONObject> kafkaDS = filteredDS.process(
                new TableProcessFunction(hbaseTag)
        );


        DataStream<JSONObject> hbaseDS = kafkaDS.getSideOutput(hbaseTag);

        hbaseDS.print("dim >>>>");
        hbaseDS.addSink(new DimSink());


        DataStreamSink<JSONObject> kafkaSink = kafkaDS.addSink(MyKafkaUtil.getKafkaSinkBySchema(new KafkaSerializationSchema<JSONObject>() {

            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObj, @Nullable Long timestamp) {
                String topic = jsonObj.getString("sink_table");
                JSONObject dataJsonObj = jsonObj.getJSONObject("data");
                return new ProducerRecord<>(topic, dataJsonObj.toJSONString().getBytes());
            }
        }));

        kafkaDS.print("fact >>>> ");

        env.execute();

    }

}
