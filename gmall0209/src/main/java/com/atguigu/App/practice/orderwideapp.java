package com.atguigu.App.practice;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.Utils.MyKafkaUtil;
import com.atguigu.bean.OrderDetail;
import com.atguigu.bean.OrderInfo;
import com.atguigu.bean.OrderWide;
import com.google.inject.internal.cglib.core.$LocalVariablesSorter;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class orderwideapp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        System.setProperty("HADOOP_USER_NAME", "atguigu");
        String orderInfoSourceTopic = "dwd_order_info";
        String  orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide";
        String groupId = "order_wide_group";

        FlinkKafkaConsumer<String> orderInfoSource = MyKafkaUtil.getKafkaSource(orderInfoSourceTopic, groupId);
        FlinkKafkaConsumer<String> orderDetailSource = MyKafkaUtil.getKafkaSource(orderDetailSourceTopic, groupId);
        DataStreamSource<String> orderInfoDS = env.addSource(orderInfoSource);
        DataStreamSource<String> orderDetailDS = env.addSource(orderDetailSource);

        // 把 jsonstr转化成指定的pojos类
        SingleOutputStreamOperator<OrderInfo> orderInfoObj = orderInfoDS.map(
                new RichMapFunction<String, OrderInfo>() {
                    SimpleDateFormat sdf;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        sdf = new SimpleDateFormat("yyyy-MM-dd");
                    }

                    @Override
                    public OrderInfo map(String value) throws Exception {
                        OrderInfo orderInfo = JSONObject.parseObject(value, OrderInfo.class);
                        orderInfo.setCreate_ts(sdf.parse(orderInfo.getCreate_time()).getTime());
                        return orderInfo;
                    }
                }
        );

        SingleOutputStreamOperator<OrderDetail> orderDetailObj = orderDetailDS.map(
                new RichMapFunction<String, OrderDetail>() {
                    SimpleDateFormat sdf;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        sdf = new SimpleDateFormat("yyyy-MM-dd");
                    }

                    @Override
                    public OrderDetail map(String value) throws Exception {
                        OrderDetail orderDetail = JSONObject.parseObject(value, OrderDetail.class);
                        orderDetail.setCreate_ts(sdf.parse(orderDetail.getCreate_time()).getTime());
                        return orderDetail;
                    }
                }
        );

        SingleOutputStreamOperator<OrderInfo> orderInfoWithWM = orderInfoObj.assignTimestampsAndWatermarks(WatermarkStrategy.<OrderInfo>forMonotonousTimestamps().withTimestampAssigner(
                new SerializableTimestampAssigner<OrderInfo>() {
                    @Override
                    public long extractTimestamp(OrderInfo element, long recordTimestamp) {
                        return element.getCreate_ts();
                    }
                }
        ));

        SingleOutputStreamOperator<OrderDetail> orderDetailwithWM = orderDetailObj.assignTimestampsAndWatermarks(WatermarkStrategy.<OrderDetail>forMonotonousTimestamps().withTimestampAssigner(
                new SerializableTimestampAssigner<OrderDetail>() {
                    @Override
                    public long extractTimestamp(OrderDetail element, long recordTimestamp) {
                        return element.getCreate_ts();
                    }
                }
        ));

        KeyedStream<OrderInfo, Long> keyedOrderInfoObj = orderInfoWithWM.keyBy(obj -> obj.getId());
        KeyedStream<OrderDetail, Long> keyedOrderDetailObj = orderDetailwithWM.keyBy(obj -> obj.getOrder_id());
        SingleOutputStreamOperator<OrderWide> orderWideObj = keyedOrderInfoObj.intervalJoin(keyedOrderDetailObj)
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo left, OrderDetail right, Context ctx, Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(left, right));
                    }
                });

//        SingleOutputStreamOperator<String> orderWideStr = orderWideObj.map(obj -> obj.toString());
//        orderWideStr.print("joined data : >>>> ");
//        FlinkKafkaProducer<String> kafkaSink = MyKafkaUtil.getKafkaSink(orderWideSinkTopic);
//        orderWideStr.addSink(kafkaSink);

        // join后的流，通过异步通信方法关联维度数据
//        AsyncDataStream.unorderedWait(orderWideObj, new asyncfunc<OrderWide>("DIM_USER_INFO") {
//
//                300, TimeUnit.SECONDS);


        AsyncDataStream.unorderedWait(orderWideObj, new asyncfunc<OrderWide>("DIM_USER_INFO") {
                    @Override
                    public String getkey(OrderWide obj) {
                        return obj.getUser_id().toString();
                    }

                    @Override
                    public void join(JSONObject dimObj, OrderWide obj) throws ParseException {
                        String birthday = dimObj.getString("BIRTHDAY");
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                        Date birthdayDate = sdf.parse(birthday);
                        Long birthTs = birthdayDate.getTime();
                        Long curTs = System.currentTimeMillis();
                        Long ageL = (curTs-birthTs)/3600L/1000L/24L/365L;
                        int age = ageL.intValue();
                        obj.setUser_age(age);
                        obj.setUser_gender(dimObj.getString("Gender"));
                    }
                }
                ,300,TimeUnit.SECONDS);

        env.execute();
    }
}
