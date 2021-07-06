package com.atguigu.App.dwm;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.Utils.DateTimeUtil;
import com.atguigu.Utils.MyKafkaUtil;
import com.atguigu.bean.OrderWide;
import com.atguigu.bean.PaymentInfo;
import com.atguigu.bean.PaymentWide;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class PaymentWideApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        System.setProperty("HADOOP_USER_NAME", "atguigu");
        String groupId = "payment_wide_group";
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSinkTopic = "dwm_payment_wide";

        FlinkKafkaConsumer<String> paymentSource = MyKafkaUtil.getKafkaSource(paymentInfoSourceTopic, groupId);
        FlinkKafkaConsumer<String> orderSource = MyKafkaUtil.getKafkaSource(orderWideSourceTopic, groupId);
        DataStreamSource<String> paymentDS = env.addSource(paymentSource);
        DataStreamSource<String> orderDS = env.addSource(orderSource);
        SingleOutputStreamOperator<PaymentInfo> paymentObjDS = paymentDS.map(str -> JSONObject.parseObject(str, PaymentInfo.class));
        SingleOutputStreamOperator<OrderWide> orderObjDS = orderDS.map(str -> JSONObject.parseObject(str, OrderWide.class));

        SingleOutputStreamOperator<PaymentInfo> paymentObjWithWm = paymentObjDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<PaymentInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((obj, ts) -> DateTimeUtil.toTs(obj.getCallback_time()))
        );
        SingleOutputStreamOperator<OrderWide> orderObjWithWm = orderObjDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<OrderWide>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((obj, ts) -> DateTimeUtil.toTs(obj.getCreate_time()))
        );

        KeyedStream<PaymentInfo, Long> keyedPaymentDS = paymentObjWithWm.keyBy(obj -> obj.getOrder_id());
        KeyedStream<OrderWide, Long> keyedOrderDS = orderObjWithWm.keyBy(obj -> obj.getOrder_id());

        SingleOutputStreamOperator<PaymentWide> joinedDS = keyedPaymentDS.intervalJoin(keyedOrderDS)
                .between(Time.seconds(-1500), Time.seconds(0))
                .process(
                        new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                            @Override
                            public void processElement(PaymentInfo left, OrderWide right, Context ctx, Collector<PaymentWide> out) throws Exception {
                                out.collect(new PaymentWide(left, right));
                            }
                        }
                );
        SingleOutputStreamOperator<String> finalDS = joinedDS.map(obj -> JSONObject.toJSONString(obj));
        finalDS.print("result :   ");

        finalDS.addSink(MyKafkaUtil.getKafkaSink(paymentWideSinkTopic));

        env.execute();
    }
}
