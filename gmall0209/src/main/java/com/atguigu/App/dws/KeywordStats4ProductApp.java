package com.atguigu.App.dws;

import com.atguigu.App.Function.KeywordProductC2RUDTF;
import com.atguigu.Utils.ClickhouseUtil;
import com.atguigu.Utils.MyKafkaUtil;
import com.atguigu.bean.KeywordStats;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class KeywordStats4ProductApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env,settings);

        String groupId = "keyword_stats_app";
        String productStatsSourceTopic ="dws_product_stats";

        tableEnv.createTemporarySystemFunction("keywordProductC2R",  KeywordProductC2RUDTF.class);

        tableEnv.executeSql("CREATE TABLE product_stats (spu_name STRING, " +
                "click_ct BIGINT," +
                "cart_ct BIGINT," +
                "order_ct BIGINT ," +
                "stt STRING,edt STRING ) " +
                "  WITH ("+ MyKafkaUtil.getKafkaDDL(productStatsSourceTopic,groupId)+")");

        //TODO 6.聚合计数
        Table keywordStatsProduct = tableEnv.sqlQuery("select keyword,ct,source, " +
                " stt,edt, UNIX_TIMESTAMP()*1000 ts from product_stats  , " +
                "LATERAL TABLE(ik_analyze(spu_name)) as T(keyword) ," +
                "LATERAL TABLE(keywordProductC2R( click_ct ,cart_ct,order_sku_num)) as T2(ct,source)");


        //TODO 7.转换为数据流
        DataStream<KeywordStats> keywordStatsProductDataStream =
                tableEnv.<KeywordStats>toAppendStream(keywordStatsProduct, KeywordStats.class);

        keywordStatsProductDataStream.print();
        //TODO 8.写入到ClickHouse
        keywordStatsProductDataStream.addSink(
                ClickhouseUtil.<KeywordStats>getJdbcSink(
                        "insert into keyword_stats_0820(keyword,ct,source,stt,edt,ts)  " +
                                "values(?,?,?,?,?,?)"));

        env.execute();
    }
}
