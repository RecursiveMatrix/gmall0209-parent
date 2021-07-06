package com.atguigu.App.practice;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.Common.GmallConfig;
import com.atguigu.bean.TableProcess;
import org.apache.avro.data.Json;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.common.protocol.types.Field;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public class tableprocessfun extends ProcessFunction<JSONObject,JSONObject> {

    OutputTag<JSONObject> hbaseTag;
    public tableprocessfun(OutputTag<JSONObject> hbase){
        this.hbaseTag = hbase;
    }

    Connection hbaseConn = null;
    Map<String, TableProcess> sqlMap = new HashMap<>();
    Set<String> existTable = new HashSet<>();


    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        hbaseConn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        fetchTable();
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                fetchTable();
            }
        },5000,5000);
    }

    private void fetchTable(){
        List<TableProcess> sqlList = mysqlutil.queryList("select * from table_process", TableProcess.class, true);
        for (TableProcess row : sqlList) {
            String sourceTable = row.getSourceTable();
            String operateType = row.getOperateType();
            String sinkType = row.getSinkType();
            String sinkTable = row.getSinkTable();
            String sqlKey = sourceTable +":"+ operateType;
            sqlMap.put(sqlKey,row);

            // TODO: BUG1
            if(operateType.equals("insert") && sinkType.equals("hbase")){
                boolean nonExist = existTable.add(sinkTable);
                if (nonExist){
                    createTable(sinkTable,row.getSinkColumns(),row.getSinkPk(),row.getSinkExtend());
                }
            }
        }
    }

    private void createTable(String tableName, String fields, String pk, String ext){
        if(pk == null){
            pk = "id";
        }
        if(ext == null){
            ext = "";
        }
        StringBuilder createSql = new StringBuilder("create table if not exists " + GmallConfig.HBASE_SCHEMA + "." + tableName + "(");
        String[] colNames = fields.split(",");
        for (int i = 0; i < colNames.length; i++) {
            String col = colNames[i];
            if(col.equals(pk)){
                createSql.append(col).append(" varchar primary key ");
            }else {
                createSql.append("info.").append(col).append(" varchar ");
            }
            if(i<colNames.length-1){
                createSql.append(",");
            }

        }
        // TODO: bug2
        createSql.append(")").append(ext);

        try {
            PreparedStatement ps = hbaseConn.prepareStatement(createSql.toString());
            ps.execute();
            ps.close();

            // TODO
//            hbaseConn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    private void filterColumn(JSONObject jsonObj ,String givenColumns){
        Set<Map.Entry<String, Object>> entries = jsonObj.entrySet();
        Iterator<Map.Entry<String, Object>> iterator = entries.iterator();
        String[] colNanes = givenColumns.split(",");
        List<String> cols = Arrays.asList(colNanes);
        for (;iterator.hasNext();){
            Map.Entry<String, Object> kv = iterator.next();
            if (!cols.contains(kv.getKey())){
                iterator.remove();
            }
        }
    }

    @Override
    public void processElement(JSONObject jsonObj, Context ctx, Collector<JSONObject> out) throws Exception {
        JSONObject data = jsonObj.getJSONObject("data");
        String table = jsonObj.getString("table");
        String type = jsonObj.getString("type");

        if (type.equals("bootstrap-insert")){
            jsonObj.put("type","insert");
        }

        if (sqlMap!=null && sqlMap.size()>0){
            String key = table +":"+type;
            TableProcess result = sqlMap.get(key);
            if (result!=null){
                String sinkTable = result.getSinkTable();
                jsonObj.put("sink_table",sinkTable);
                if (result.getSinkColumns()!=null && result.getSinkColumns().length()>0){
                    filterColumn(data,result.getSinkColumns());
                }else {
                    System.out.println("no such key");
                }
            }
            if (result!=null && result.getSinkType().equals(TableProcess.SINK_TYPE_HBASE)){
                ctx.output(hbaseTag,jsonObj);
            }else if (result!=null && result.getSinkType().equals(TableProcess.SINK_TYPE_KAFKA)){
                out.collect(jsonObj);
            }
        }
        }



}
