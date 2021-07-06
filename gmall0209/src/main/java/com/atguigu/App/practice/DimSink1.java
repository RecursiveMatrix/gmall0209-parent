package com.atguigu.App.practice;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.Common.GmallConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Collection;
import java.util.Set;

public class DimSink1 extends RichSinkFunction<JSONObject> {
    Connection hbase = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        hbase = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    @Override
    public void invoke(JSONObject jsonObj, Context context) throws Exception {
        JSONObject data = jsonObj.getJSONObject("data");
        String sink_table = jsonObj.getString("sink_table");
        String createSql = genSql(sink_table,data);
        PreparedStatement ps = hbase.prepareStatement(createSql);
        ps.executeUpdate();
        hbase.commit();
        ps.close();

    }

    private String genSql(String tableName,JSONObject data){
        Set<String> cols = data.keySet();
        Collection<Object> values = data.values();
        String upsertSql = "upsert into "+GmallConfig.HBASE_SCHEMA+"."+tableName+"("+StringUtils.join(cols,",") + ")";
        String valueSql = " values ('" + StringUtils.join(values,"','") +"')";
        return upsertSql + valueSql;
    }
}
