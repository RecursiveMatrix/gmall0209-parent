package com.atguigu.App.dws;

import com.atguigu.App.Function.KeywordUDTF;
import com.atguigu.Common.GmallConstant;
import com.atguigu.Utils.ClickhouseUtil;
import com.atguigu.Utils.MyKafkaUtil;
import com.atguigu.bean.KeywordStats;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


// 统计搜索关键词

public class KeywordStatsApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        System.setProperty("HADOOP_USER_NAME","atguigu");

        EnvironmentSettings setting = EnvironmentSettings.newInstance()
                .inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, setting);

        String groupId = "keyword_stats_app";
        String pageViewSourceTopic ="dwd_page_log";

        // 使用UDTF前需要先注册
        tableEnv.createTemporarySystemFunction("ik_analyze", KeywordUDTF.class);

//        创建动态表, 获取 page中的
        tableEnv.executeSql("CREATE TABLE page_view ( " +
                "common MAP<STRING,STRING>, " +
                "page MAP<STRING,STRING>, " +
                "ts BIGINT, " +
                "rowtime AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000, 'yyyy-MM-dd HH:mm:ss')), " +
                "WATERMARK FOR rowtime AS rowtime - INTERVAL '2' SECOND) " +
                "WITH ("+ MyKafkaUtil.getKafkaDDL(pageViewSourceTopic,groupId) +")");

//        从动态表中查询数据，定位搜索关键词
        Table fullwordView = tableEnv.sqlQuery("SELECT page['item'] fullword, rowtime " +
                "from page_view " +
                "where page['page_id']='good_list' and page['item'] IS NOT NULL");

//        对结果中的关键词进行拆分（自定义函数）：查询结果和查询结果的分词做 intervalJoin
        Table keywordTable = tableEnv.sqlQuery("select keyword,rowtime  from " + fullwordView + " ," +
                " LATERAL TABLE(ik_analyze(fullword)) as T(keyword)");


        //        聚合
        Table keywordStatsSearch = tableEnv.sqlQuery("SELECT keyword, count(*) ct, '"
                + GmallConstant.KEYWORD_SEARCH + "' source," +
                "DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt, " +
                "DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt, " +
                "UNIX_TIMESTAMP()*1000 ts " +
                "FROM " + keywordTable +
                " group by TUMBLE(rowtime, INTERVAL '10' SECOND), keyword");


                // 将table转化为流
        DataStream<KeywordStats> keywordStatsDS = tableEnv.toAppendStream(keywordStatsSearch, KeywordStats.class);
        keywordStatsDS.addSink(ClickhouseUtil.getJdbcSink(
                "insert into keyword_stats_0820 (keyword,ct,source,stt,edt,ts) values(?,?,?,?,?,?)"
        ));



        // 最后写入到clickhouse
        env.execute();
    }
}
