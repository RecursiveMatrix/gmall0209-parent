package com.atguigu.App.dws;

import com.atguigu.Utils.ClickhouseUtil;
import com.atguigu.Utils.MyKafkaUtil;
import com.atguigu.bean.ProvinceStats;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ProvinceStatsSqlApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        String groupId = "province_stats";
        String orderWideTopic = "dwm_order_wide";

        // 建表语句：指定某一列为rowtime，WATERMARK FOR rowtime as rowtime， + kafka配置
        tableEnv.executeSql("CREATE TABLE ORDER_WIDE (province_id BIGINT, " +
                "province_name STRING,province_area_code STRING" +
                ",province_iso_code STRING,province_3166_2_code STRING,order_id STRING, " +
                "split_total_amount DOUBLE,create_time STRING,rowtime AS TO_TIMESTAMP(create_time) ," +
                "WATERMARK FOR  rowtime  AS rowtime)" +
                " WITH (" + MyKafkaUtil.getKafkaDDL(orderWideTopic, groupId) + ")");

        Table provinceStateTable = tableEnv.sqlQuery("select " +
                "DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND ),'yyyy-MM-dd HH:mm:ss') stt, " +
                "DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND ),'yyyy-MM-dd HH:mm:ss') edt , " +
                " province_id,province_name,province_area_code area_code," +
                "province_iso_code iso_code ,province_3166_2_code iso_3166_2 ," +
                "COUNT( DISTINCT  order_id) order_count, sum(split_total_amount) order_amount," +
                "UNIX_TIMESTAMP()*1000 ts "+
                " from ORDER_WIDE group by TUMBLE(rowtime, INTERVAL '10' SECOND )," +
                " province_id,province_name,province_area_code,province_iso_code,province_3166_2_code ");

        DataStream<ProvinceStats> provinceStatsDataStream =
                tableEnv.toAppendStream(provinceStateTable, ProvinceStats.class);
        provinceStatsDataStream.print("地区统计： >>>>> ");
        provinceStatsDataStream.addSink(ClickhouseUtil.
                <ProvinceStats>getJdbcSink("insert into  province_stats_0820  values(?,?,?,?,?,?,?,?,?,?)"));

        env.execute();
    }
}
