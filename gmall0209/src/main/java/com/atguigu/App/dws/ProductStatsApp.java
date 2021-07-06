package com.atguigu.App.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.App.Function.DimAsyncFunction;
import com.atguigu.Common.GmallConstant;
import com.atguigu.Utils.ClickhouseUtil;
import com.atguigu.Utils.DateTimeUtil;
import com.atguigu.Utils.MyKafkaUtil;
import com.atguigu.bean.OrderWide;
import com.atguigu.bean.PaymentWide;
import com.atguigu.bean.ProductStats;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

public class ProductStatsApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        System.setProperty("HADOOP_USER_NAME","atguigu");

        String groupId = "product_stats_app";

        String pageViewSourceTopic = "dwd_page_log";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSourceTopic = "dwm_payment_wide";
        String cartInfoSourceTopic = "dwd_cart_info";
        String favorInfoSourceTopic = "dwd_favor_info";
        String refundInfoSourceTopic = "dwd_order_refund_info";
        String commentInfoSourceTopic = "dwd_comment_info";

        FlinkKafkaConsumer<String> pvSource = MyKafkaUtil.getKafkaSource(pageViewSourceTopic, groupId);
        FlinkKafkaConsumer<String> orderSource = MyKafkaUtil.getKafkaSource(orderWideSourceTopic, groupId);
        FlinkKafkaConsumer<String> paymentSource = MyKafkaUtil.getKafkaSource(paymentWideSourceTopic, groupId);
        FlinkKafkaConsumer<String> cartSource = MyKafkaUtil.getKafkaSource(cartInfoSourceTopic, groupId);
        FlinkKafkaConsumer<String> favorSource = MyKafkaUtil.getKafkaSource(favorInfoSourceTopic, groupId);
        FlinkKafkaConsumer<String> refundSource = MyKafkaUtil.getKafkaSource(refundInfoSourceTopic, groupId);
        FlinkKafkaConsumer<String> commentSource = MyKafkaUtil.getKafkaSource(commentInfoSourceTopic, groupId);

        DataStreamSource<String> pvDS = env.addSource(pvSource);
        DataStreamSource<String> orderDS = env.addSource(orderSource);
        DataStreamSource<String> paymentDS = env.addSource(paymentSource);
        DataStreamSource<String> cartDS = env.addSource(cartSource);
        DataStreamSource<String> favorDS = env.addSource(favorSource);
        DataStreamSource<String> refundDS = env.addSource(refundSource);
        DataStreamSource<String> commentDS = env.addSource(commentSource);

        SingleOutputStreamOperator<ProductStats> pageAndDisplaysObjDS = pvDS.process(
                new ProcessFunction<String, ProductStats>() {
                    @Override
                    public void processElement(String jsonStr, Context ctx, Collector<ProductStats> out) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        // 在页面数据中统计 sku个数
                        JSONObject pageObj = jsonObj.getJSONObject("page");
                        String page_id = pageObj.getString("page_id");
                        if (page_id == null || page_id.length() == 0) {
                            System.out.println("no page id :>>>>>> "+jsonObj);
                        }
                        Long ts = jsonObj.getLong("ts");
                        if (page_id.equals("good_detail")) {
                            Long skuId = pageObj.getLong("item");
                            ProductStats productStats = ProductStats.builder().sku_id(skuId).click_ct(1L).ts(ts).build();
                            out.collect(productStats);
                        }

                        // 在曝光数据中统计曝光次数
                        JSONArray displays = jsonObj.getJSONArray("display");
                        if (displays != null && displays.size() > 0) {
                            for (int i = 0; i < displays.size(); i++) {
                                JSONObject display = displays.getJSONObject(i);
                                if (display.getString("item_type").equals("sku_id")) {
                                    Long skuId = display.getLong("item");
                                    ProductStats productStats = ProductStats.builder().sku_id(skuId).display_ct(1L).ts(ts).build();
                                    out.collect(productStats);
                                }
                            }
                        }
                    }
                }
        );

        SingleOutputStreamOperator<ProductStats> orderObjDS = orderDS.map(
                jsonStr -> {
                    OrderWide orderObj = JSON.parseObject(jsonStr, OrderWide.class);
                    String create_time = orderObj.getCreate_time();
                    Long ts = DateTimeUtil.toTs(create_time);
                    return ProductStats.builder().sku_id(orderObj.getSku_id())
                            .orderIdSet(new HashSet(Collections.singleton(orderObj.getOrder_id())))
                            .order_sku_num(orderObj.getSku_num())
                            .order_amount(orderObj.getSplit_total_amount())
                            .ts(ts).build();
                }
        );

        SingleOutputStreamOperator<ProductStats> paymentObjDS = paymentDS.map(
                jsonStr -> {
                    PaymentWide paymentWide = JSON.parseObject(jsonStr, PaymentWide.class);
                    Long ts = DateTimeUtil.toTs(paymentWide.getPayment_create_time());
                    return ProductStats.builder().sku_id(paymentWide.getSku_id())
                            .payment_amount(paymentWide.getSplit_total_amount())
                            .paidOrderIdSet(new HashSet(Collections.singleton(paymentWide.getOrder_id())))
                            .ts(ts).build();
                }
        );

        SingleOutputStreamOperator<ProductStats> favorObjDS = favorDS.map(
                jsonStr -> {
                    JSONObject jsonObj = JSON.parseObject(jsonStr);
                    String create_time = jsonObj.getString("create_time");
                    Long ts = DateTimeUtil.toTs(create_time);
                    ProductStats produceStats = ProductStats.builder()
                            .sku_id(jsonObj.getLong("sku_id"))
                            .favor_ct(1L)
                            .ts(ts).build();
                    return produceStats;
                }
        );

        SingleOutputStreamOperator<ProductStats> cartObjDS = cartDS.map(
                jsonStr -> {
                    JSONObject jsonObj = JSON.parseObject(jsonStr);
                    Long ts = DateTimeUtil.toTs(jsonObj.getString("create_time"));
                    return ProductStats.builder()
                            .sku_id(jsonObj.getLong("sku_id"))
                            .cart_ct(1L).ts(ts).build();
                }
        );



        SingleOutputStreamOperator<ProductStats> refundObjDS = refundDS.map(
                jsonStr -> {
                    JSONObject jsonObj = JSON.parseObject(jsonStr);
                    Long ts = DateTimeUtil.toTs(jsonObj.getString("create_time"));
                    ProductStats productStats = ProductStats.builder()
                            .sku_id(jsonObj.getLong("sku_id"))
                            .refund_amount(jsonObj.getBigDecimal("refund_amount"))
                            .refundOrderIdSet(new HashSet(Collections.singleton(jsonObj.getLong("order_id"))))
                            .ts(ts).build();
                    return productStats;
                }
        );

        SingleOutputStreamOperator<ProductStats> commentObjDS = commentDS.map(
                jsonStr -> {
                    JSONObject jsonObj = JSONObject.parseObject(jsonStr);
                    Long ts = DateTimeUtil.toTs(jsonObj.getString("create_time"));
                    Long goodCt = GmallConstant.APPRAISE_GOOD.equals(jsonObj.getString("appraise")) ? 1L : 0L;
                    ProductStats productStats = ProductStats.builder()
                            .sku_id(jsonObj.getLong("sku_id"))
                            .comment_ct(1L).good_comment_ct(goodCt).ts(ts).build();
                    return productStats;
                }
        );

        DataStream<ProductStats> unionDS = pageAndDisplaysObjDS.union(
                orderObjDS, paymentObjDS,
                favorObjDS, cartObjDS, refundObjDS, commentObjDS
        );

        SingleOutputStreamOperator<ProductStats> unionDSwithWM = unionDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<ProductStats>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<ProductStats>() {
                                    @Override
                                    public long extractTimestamp(ProductStats element, long recordTimestamp) {
                                        return element.getTs();
                                    }
                                }
                        )
        );

        KeyedStream<ProductStats, Long> keyedDS = unionDSwithWM.keyBy(obj -> obj.getSku_id());
        WindowedStream<ProductStats, Long, TimeWindow> windowDS = keyedDS.window(TumblingEventTimeWindows.of(Time.seconds(10)));

        SingleOutputStreamOperator<ProductStats> resultDS = windowDS.reduce(
                new ReduceFunction<ProductStats>() {
                    @Override
                    public ProductStats reduce(ProductStats stats1, ProductStats stats2) throws Exception {
                        stats1.setDisplay_ct(stats1.getDisplay_ct() + stats2.getDisplay_ct());
                        stats1.setClick_ct(stats1.getClick_ct() + stats2.getClick_ct());
                        stats1.setCart_ct(stats1.getCart_ct() + stats2.getCart_ct());
                        stats1.setFavor_ct(stats1.getFavor_ct() + stats2.getFavor_ct());
                        stats1.setOrder_amount(stats1.getOrder_amount().add(stats2.getOrder_amount()));

                        stats1.getOrderIdSet().addAll(stats2.getOrderIdSet());
                        stats1.setOrder_ct(stats1.getOrderIdSet().size() + 0L);

                        stats1.setOrder_sku_num(stats1.getOrder_sku_num() + stats2.getOrder_sku_num());
                        stats1.setPayment_amount(stats1.getPayment_amount().add(stats2.getPayment_amount()));

                        stats1.getRefundOrderIdSet().addAll(stats2.getRefundOrderIdSet());
                        stats1.setRefund_order_ct(stats1.getRefundOrderIdSet().size() + 0L);
                        stats1.setRefund_amount(stats1.getRefund_amount().add(stats2.getRefund_amount()));

                        stats1.getPaidOrderIdSet().addAll(stats2.getPaidOrderIdSet());
                        stats1.setPaid_order_ct(stats1.getPaidOrderIdSet().size() + 0L);

                        stats1.setComment_ct(stats1.getComment_ct() + stats2.getComment_ct());
                        stats1.setGood_comment_ct(stats1.getGood_comment_ct() + stats2.getGood_comment_ct());

                        return stats1;
                    }
                },
                new ProcessWindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
                    @Override
                    public void process(Long key, Context context, Iterable<ProductStats> productStats, Collector<ProductStats> out) throws Exception {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        for (ProductStats productStat : productStats) {
                            productStat.setStt(sdf.format(new Date(context.window().getStart())));
                            productStat.setEdt(sdf.format(new Date(context.window().getEnd())));
                            productStat.setTs(new Date().getTime());
                            out.collect(productStat);
                        }
                    }
                }
        );

        SingleOutputStreamOperator<ProductStats> relatedSKU = AsyncDataStream.unorderedWait(
                resultDS,
                new DimAsyncFunction<ProductStats>("DIM_SKU_INFO") {
                    @Override
                    public String getKey(ProductStats productStats) {
                        return productStats.getSku_id().toString();
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimInfoObj) throws ParseException {
                        productStats.setSku_name(dimInfoObj.getString("SKU_NAME"));
                        productStats.setSku_price(dimInfoObj.getBigDecimal("PRICE"));
                        productStats.setSpu_id(dimInfoObj.getLong("SPU_ID"));
                        productStats.setTm_id(dimInfoObj.getLong("TM_ID"));
                        productStats.setCategory3_id(dimInfoObj.getLong("CATEGORY3_ID"));
                    }
                },
                60, TimeUnit.SECONDS
        );

        SingleOutputStreamOperator<ProductStats> relatedSPU = AsyncDataStream.unorderedWait(
                relatedSKU,
                new DimAsyncFunction<ProductStats>("DIM_SPU_INFO") {
                    @Override
                    public String getKey(ProductStats productStats) {
                        return productStats.getSpu_id().toString();
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimInfoObj) throws ParseException {
                        productStats.setSpu_name(dimInfoObj.getString("SPU_NAME"));
                    }
                },
                60,TimeUnit.SECONDS
        );

        relatedSPU.print("spu : >>>> ");

        SingleOutputStreamOperator<ProductStats> relatedCategory = AsyncDataStream.unorderedWait(
                relatedSPU,
                new DimAsyncFunction<ProductStats>("DIM_BASE_CATEGORY3") {
                    @Override
                    public String getKey(ProductStats productStats) {
                        return productStats.getCategory3_id().toString();
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimInfoObj) throws ParseException {
                        productStats.setCategory3_name(dimInfoObj.getString("NAME"));
                    }
                },
                60, TimeUnit.SECONDS
        );

        SingleOutputStreamOperator<ProductStats> relatedTM = AsyncDataStream.unorderedWait(
                relatedCategory,
                new DimAsyncFunction<ProductStats>("DIM_BASE_TRADEMARK") {
                    @Override
                    public String getKey(ProductStats productStats) {
                        return productStats.getTm_id().toString();
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimInfoObj) throws ParseException {
                        productStats.setTm_name(dimInfoObj.getString("TM_NAME"));
                    }
                },
                60, TimeUnit.SECONDS
        );

        relatedTM.addSink(
                ClickhouseUtil.<ProductStats>getJdbcSink(
                        "insert into product_stats_0820 values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
        );


        env.execute();
    }
}
