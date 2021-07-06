package com.atguigu.App.Function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.Common.GmallConfig;
import com.atguigu.Utils.DimUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import java.sql.*;
import java.util.Set;

// 更新 HBase中配置表信息： 通过获取输入到 hbase数据流中的 数据和目标表， 拼出 sql，用它来更新表信息
public class DimSink extends RichSinkFunction<JSONObject> {
    Connection connection = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    @Override
    public void invoke(JSONObject jsonObject, Context context) throws Exception {

        String tableName = jsonObject.getString("sink_table");
        JSONObject dataJsonObj = jsonObject.getJSONObject("data");
        if (dataJsonObj != null && dataJsonObj.size() > 0) {
            String upsertSql = genUpsertSql(tableName.toUpperCase(), jsonObject.getJSONObject("data"));
            try {
                System.out.println(upsertSql);
                PreparedStatement ps = connection.prepareStatement(upsertSql);

                // TODO: bug 自动提交
                ps.executeUpdate();
                connection.commit();
                ps.close();
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException("执行sql失败！");
            }
        }

        // 如果维度数据发生变化，在redis中清除
        if(jsonObject.getString("type").equals("update") || jsonObject.getString("type").equals("delete")){
            DimUtil.deleteCached(tableName,dataJsonObj.getString("id"));
        }

    }

    public String genUpsertSql(String tableName, JSONObject jsonObject) {
        Set<String> fields = jsonObject.keySet();
        System.out.println(fields);
        String upsertSql = "upsert into " + GmallConfig.HBASE_SCHEMA + "." + tableName + "(" + StringUtils.join(fields, ",") + ")";
        // TODO: bug 引入值的时候不要忘记单引号
        String valuesSql = " values ('" + StringUtils.join(jsonObject.values(), "','") + "')";
        return upsertSql + valuesSql;
    }
}

