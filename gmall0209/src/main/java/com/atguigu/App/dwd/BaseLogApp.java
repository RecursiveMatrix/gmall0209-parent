package com.atguigu.App.dwd;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.Utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.util.Date;

public class BaseLogApp {

    private static final String TOPIC_START ="dwd_start_log";
    private static final String TOPIC_PAGE ="dwd_page_log";
    private static final String TOPIC_DISPLAY ="dwd_display_log";

    public static void main(String[] args) throws Exception {

        // TODO 1. 基本配置：获取环境，并行度与 kafka 一致，启用并设置检查点和其位置，超时
        System.setProperty("HADOOP_USER_NAME","atguigu");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
//        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10000);
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop201:9820/gmall/flink/checkpoint"));


        // TODO 2. 从 kafka 中获取指定 topic 的数据
        String groupId = "ods_dwd_base_log_app";
        String topic = "ods_base_log";

        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);

        // TODO 3. 进行数据转化： 从 kafka 的 string --> json 对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(str -> JSONObject.parseObject(str));

        // TODO 4. 新老访客确认需求： 保存每个 mid 的首次访问日期到状态中，每次数据传过来是都进行日期比较：如果有状态并且新数据日期比当前保存日期小，可认为是老访客，否则为新访客；另外，如果没有状态保存，则更新状态

        KeyedStream<JSONObject, String> keyedJsonObjDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));
        SingleOutputStreamOperator<JSONObject> filteredDS = keyedJsonObjDS.map(
                new RichMapFunction<JSONObject, JSONObject>() {

                    private ValueState<String> firstVisitedDateState;
                    private SimpleDateFormat sdf;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        firstVisitedDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("firstVistedDate", String.class));
                        sdf = new SimpleDateFormat("yyyyMMdd");
                    }

                    @Override
                    public JSONObject map(JSONObject jsonObj) throws Exception {
                        Long ts = jsonObj.getLong("ts");
                        String isNew = jsonObj.getJSONObject("common").getString("is_new");
                        if (isNew.equals("1")) {
                            String realDate = sdf.format(new Date(ts));
                            String recordDate = firstVisitedDateState.value();
                            if (recordDate != null && recordDate.length() != 0) {
                                jsonObj.getJSONObject("common").put("is_new", "0");
                            } else {
                                firstVisitedDateState.update(realDate);
                            }
                        }
                        return jsonObj;
                    }
                }
        );
//        filteredDS.print();

        // TODO 5. 分流成启动日志数据流，页面日志流，曝光日志流，其中页面日志保留在主流，另外两个侧输出流形式输出, 由于写回到 kafka，需要把数据类型转化为 string
        OutputTag<String> startTag = new OutputTag<String>("start") {};
        OutputTag<String> displayTag = new OutputTag<String>("displays") {};

        // 将 jsonObj 流转化成 string 流，先筛选出 start 日志，再筛选 display，由于一条数据中可能有多个 display，需要遍历，同时在 display 中加入 page_id 属性
        SingleOutputStreamOperator<String> splitDS = filteredDS.process(
                new ProcessFunction<JSONObject, String>() {
                    @Override
                    public void processElement(JSONObject jsonObj, Context ctx, Collector<String> out) throws Exception {
                        JSONObject startJsonObj = jsonObj.getJSONObject("start");
                        String dataStr = jsonObj.toString();
                        if (startJsonObj != null && startJsonObj.size() > 0) {
                            ctx.output(startTag, dataStr);
                        } else {
                            out.collect(dataStr);
                            JSONArray displayJsonArray = jsonObj.getJSONArray("displays");
                            if(displayJsonArray!=null && displayJsonArray.size()>0){
                                for (int i = 0; i < displayJsonArray.size(); i++) {
                                    JSONObject displayJSONObject = displayJsonArray.getJSONObject(i);
                                    String pageId = jsonObj.getJSONObject("page").getString("page_id");
                                    displayJSONObject.put("page_id", pageId);
                                    ctx.output(displayTag, displayJSONObject.toString());
                                }
                            }
                        }
                    }
                }
        );

        DataStream<String> startLogDS = splitDS.getSideOutput(startTag);
        DataStream<String> displayLogDS = splitDS.getSideOutput(displayTag);

        splitDS.print("page >>>>");
        splitDS.getSideOutput(startTag).print("start >>>>");
        splitDS.getSideOutput(displayTag).print("display >>>>");

        // TODO 6. 针对不同的流添加 sink： 获取写入到 kafka 的位置，再把流跟该 sink 关联
        FlinkKafkaProducer<String> startLogSink = MyKafkaUtil.getKafkaSink(TOPIC_START);
        FlinkKafkaProducer<String> displayLogSink = MyKafkaUtil.getKafkaSink(TOPIC_DISPLAY);
        FlinkKafkaProducer<String> pageLogSink = MyKafkaUtil.getKafkaSink(TOPIC_PAGE);

        splitDS.addSink(pageLogSink);
        startLogDS.addSink(startLogSink);
        displayLogDS.addSink(displayLogSink);


        env.execute();

    }
}
