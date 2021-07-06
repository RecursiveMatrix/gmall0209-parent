package com.atguigu.App.Function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.Common.GmallConfig;
import com.atguigu.Utils.MySQLUtil;
import com.atguigu.bean.TableProcess;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public class TableProcessFunction1 extends ProcessFunction<JSONObject,JSONObject> {

    // todo 1.定义侧输出流标签以及构造方法
    OutputTag<JSONObject> hbaseTag;
    public TableProcessFunction1(OutputTag<JSONObject> hbaseTag){
        this.hbaseTag = hbaseTag;
    }

    // todo 2.初始化方法
    Connection hbaseConn = null;
    Map<String,TableProcess> cacheMap = new HashMap<>();
    Set<String> existedTable = new HashSet<>();

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        hbaseConn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        initTable();
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                initTable();
            }
        },5000,5000);
    }


    // todo 3.从hbase数据库中通过 mySqlUtil 来获得包装后的表信息
    private void initTable(){
        try {
            List<TableProcess> tableProcessesList = MySQLUtil.queryList("select * from table_process", TableProcess.class, true);
            for (TableProcess tableProcess : tableProcessesList) {
                String sourceTable = tableProcess.getSourceTable();
                String operateType = tableProcess.getOperateType();
                String sinkTable = tableProcess.getSinkTable();
                String sinkType = tableProcess.getSinkType();
                String key = sourceTable +":"+ operateType;
                cacheMap.put(key,tableProcess);
                if("hbase".equals(sinkTable) && "insert".equals(operateType)){
                    boolean nonExist = existedTable.add(sinkTable);
                    if(nonExist){
                        createTable(sinkTable,tableProcess.getSinkColumns(),tableProcess.getSinkPk(),tableProcess.getSinkExtend());
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    // todo 4. 如果 hbase中不存在当前的表，构造 sql语句建表
    private void createTable(String tableName, String fields, String pk, String ext){
        if(pk == null){
            pk = "id";
        }
        if(ext == null){
            ext = "";
        }
        StringBuilder createSql = new StringBuilder("create table if not exist " + GmallConfig.HBASE_SCHEMA + "." + tableName + "(");
        String[] colNames = fields.split(",");
        for (int i = 0; i < fields.length(); i++) {
            String colName = colNames[i];
            if(colName.equals(pk)){
                createSql.append(colName).append(" varchar primary key");
            }else {
                createSql.append("info.").append(colName).append(" varchar ");
            }
            if(i<fields.length()-1){
                createSql.append(",");
            }
        }
        createSql.append(")");
        createSql.append(ext);

        try {
            PreparedStatement pstmt = hbaseConn.prepareStatement(createSql.toString());
            pstmt.execute();
            pstmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    // todo 5. 根据当前json对象数据以及hbase表中字段过滤掉不必要的数据
    private void filterColumns(JSONObject jsonObj, String fields){
        String[] colNames = fields.split(",");
        List<String> colCollections = Arrays.asList(colNames);
        Set<Map.Entry<String, Object>> entries = jsonObj.entrySet();
        Iterator<Map.Entry<String, Object>> iterator = entries.iterator();
        for(;iterator.hasNext();){
            if(colCollections.contains(iterator.next().getKey())){
                iterator.remove();
            }
        }
    }


    @Override
    public void processElement(JSONObject jsonObj, Context ctx, Collector<JSONObject> out) throws Exception {
        String table = jsonObj.getString("table");
        JSONObject data = jsonObj.getJSONObject("data");
        String type = jsonObj.getString("type");

        if(type.equals("bootstrap-insert")){
            jsonObj.put("type","insert");
        }

        String key = table+":"+type;
        if(cacheMap!=null && cacheMap.size()>0){
            TableProcess tableProcess = cacheMap.get(key);
            String sinkTable = tableProcess.getSinkTable();
            jsonObj.put("sink_table",sinkTable);
            if(tableProcess.getSinkColumns()!=null && tableProcess.getSinkColumns().length()>0){
                filterColumns(data,tableProcess.getSinkColumns());
            }else {
                System.out.println("no such key"+key);
            }
            if(tableProcess!=null && tableProcess.getSinkType().equals(TableProcess.SINK_TYPE_HBASE)){
                ctx.output(hbaseTag,jsonObj);
            }else if(tableProcess!=null && tableProcess.getSinkType().equals(TableProcess.SINK_TYPE_KAFKA)){
                out.collect(jsonObj);
            }
        }
    }
}
