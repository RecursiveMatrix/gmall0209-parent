package com.atguigu.App.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.App.Function.DimAsyncFunction;
import com.atguigu.Utils.MyKafkaUtil;
import com.atguigu.bean.OrderDetail;
import com.atguigu.bean.OrderInfo;
import com.atguigu.bean.OrderWide;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
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
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class OrderWideApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        String orderInfoSourceTopic = "dwd_order_info";
        String  orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide";
        String groupId = "order_wide_group";

        FlinkKafkaConsumer<String> orderInfoKafka = MyKafkaUtil.getKafkaSource(orderInfoSourceTopic, groupId);
        FlinkKafkaConsumer<String> orderDetailKafka = MyKafkaUtil.getKafkaSource(orderDetailSourceTopic, groupId);
        DataStreamSource<String> orderInfoSource = env.addSource(orderInfoKafka);
        DataStreamSource<String> orderDetailSource = env.addSource(orderDetailKafka);

        // 包装 orderInfo，通过 map方法根据 create time添加 ts
        SingleOutputStreamOperator<OrderInfo> orderInfoDS = orderInfoSource.map(
                new RichMapFunction<String, OrderInfo>() {
                    SimpleDateFormat sdf = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        sdf = new SimpleDateFormat("yyyy-MM-dd");
                    }

                    @Override
                    public OrderInfo map(String jsonString) throws Exception {
                        OrderInfo orderInfo = JSON.parseObject(jsonString, OrderInfo.class);
                        orderInfo.setCreate_ts(sdf.parse(orderInfo.getCreate_time()).getTime());
                        return orderInfo;
                    }
                }
        );

        SingleOutputStreamOperator<OrderDetail> orderDetailDS = orderDetailSource.map(
                new RichMapFunction<String, OrderDetail>() {
                    SimpleDateFormat sdf = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        sdf = new SimpleDateFormat("yyyy-MM-dd");
                    }

                    @Override
                    public OrderDetail map(String jsonString) throws Exception {
                        OrderDetail orderDetail = JSON.parseObject(jsonString, OrderDetail.class);
                        orderDetail.setCreate_ts(sdf.parse(orderDetail.getCreate_time()).getTime());
                        return orderDetail;
                    }
                }
        );

        SingleOutputStreamOperator<OrderInfo> orderInfowithWM = orderInfoDS.assignTimestampsAndWatermarks(WatermarkStrategy.<OrderInfo>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
            @Override
            public long extractTimestamp(OrderInfo orderInfo, long recordTimestamp) {
                return orderInfo.getCreate_ts();
            }
        }));

        SingleOutputStreamOperator<OrderDetail> orderDetailwithWM = orderDetailDS.assignTimestampsAndWatermarks(WatermarkStrategy.<OrderDetail>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
            @Override
            public long extractTimestamp(OrderDetail orderDetail, long recordTimestamp) {
                return orderDetail.getCreate_ts();
            }
        }));

        KeyedStream<OrderInfo, Long> keyedOrderInfowithWM = orderInfowithWM.keyBy(obj -> obj.getId());
        KeyedStream<OrderDetail, Long> keyedOrderDetailwithWM = orderDetailwithWM.keyBy(obj -> obj.getOrder_id());

        // 通过 intervalJoin做事实表的关联
        SingleOutputStreamOperator<OrderWide> orderWideDS = keyedOrderInfowithWM.intervalJoin(keyedOrderDetailwithWM)
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, Context ctx, Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(orderInfo, orderDetail));
                    }
                });


        // 通过异步通讯的方式处理事实和维度表的关联，传入数据流，异步通讯功能，超时时间
        /* 异步通信功能：传入需要关联的维度表，初始化ExecutorThreadPool，在调用方法里面使用线程的submit方法；
            由于每个维度表的主键不同，关联方式也不同，需要针对不同维度表重写获取主键和 join方法

         */

        SingleOutputStreamOperator<OrderWide> orderWideWithUserDS = AsyncDataStream.unorderedWait(
                orderWideDS,
                new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {

                    @Override
                    public String getKey(OrderWide orderwide) {
                        return orderwide.getUser_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderwide, JSONObject dimInfoObj) throws ParseException {
                        String birthday = dimInfoObj.getString("BIRTHDAY");
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                        Date birthDate = sdf.parse(birthday);
                        Long birthTs = birthDate.getTime();
                        Long curTs = System.currentTimeMillis();
                        Long ageTs = curTs - birthTs;
                        Long ageLong = ageTs / 1000L / 3600L / 24L / 365L;
                        int age = ageLong.intValue();

                        orderwide.setUser_age(age);
                        orderwide.setUser_gender(dimInfoObj.getString("GENDER"));

                    }
                },
                60, TimeUnit.SECONDS);

        SingleOutputStreamOperator<OrderWide> orderWideWithProvinceDS = AsyncDataStream.unorderedWait(
                orderWideWithUserDS,
                new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getProvince_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfoObj) throws ParseException {
                        String province_name = dimInfoObj.getString("NAME");
                        String province_area_code = dimInfoObj.getString("AREA_CODE");
                        String province_iso_code = dimInfoObj.getString("ISO_CODE");
                        String province_3166_2_code = dimInfoObj.getString("ISO_3166_2");
                        orderWide.setProvince_name(province_name);
                        orderWide.setProvince_area_code(province_area_code);
                        orderWide.setProvince_iso_code(province_iso_code);
                        orderWide.setProvince_3166_2_code(province_3166_2_code);
                    }
                }
                , 30, TimeUnit.SECONDS);

        SingleOutputStreamOperator<OrderWide> orderWideWithSkuDS = AsyncDataStream.unorderedWait(
                orderWideWithProvinceDS,
                new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getSku_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfoObj) throws ParseException {
                        orderWide.setSku_name(dimInfoObj.getString("SKU_NAME"));
                        orderWide.setSpu_id(dimInfoObj.getLong("SPU_ID"));
                        orderWide.setCategory3_id(dimInfoObj.getLong("CATEGORY3_ID"));
                        orderWide.setTm_id(dimInfoObj.getLong("TM_ID"));
                    }
                },
                30,
                TimeUnit.SECONDS
        );

        SingleOutputStreamOperator<OrderWide> orderWideWithSpuDS = AsyncDataStream.unorderedWait(
                orderWideWithSkuDS,
                new DimAsyncFunction<OrderWide>("DIM_SPU_INFO") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getSpu_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfoObj) throws ParseException {
                        orderWide.setSpu_name(dimInfoObj.getString("SPU_NAME"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        SingleOutputStreamOperator<OrderWide> orderWideWithTnameDS = AsyncDataStream.unorderedWait(
                orderWideWithSpuDS,
                new DimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getTm_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfoObj) throws ParseException {
                        orderWide.setTm_name(dimInfoObj.getString("TM_NAME"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        SingleOutputStreamOperator<OrderWide> orderWideWithCategory = AsyncDataStream.unorderedWait(
                orderWideWithTnameDS,
                new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getCategory3_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfoObj) throws ParseException {
                        orderWide.setCategory3_name(dimInfoObj.getString("NAME"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        // 将关联后的订单宽表写回到 kafka的 DWM层
        SingleOutputStreamOperator<String> finalDS = orderWideWithCategory.map(obj -> JSON.toJSONString(obj));
        finalDS.print();
        finalDS.addSink(MyKafkaUtil.getKafkaSink(orderWideSinkTopic));


        env.execute();

    }
}
