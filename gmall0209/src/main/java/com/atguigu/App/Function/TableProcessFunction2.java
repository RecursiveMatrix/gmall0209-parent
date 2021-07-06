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

public class TableProcessFunction2 extends ProcessFunction<JSONObject,JSONObject> {

    OutputTag<JSONObject> dimTag;
    public TableProcessFunction2(OutputTag dimTag){
        this.dimTag = dimTag;
    }

    // 声明一个set集合在内存中记录配置表表名，map记录配置表中的信息
    Set<String> existTable = new HashSet<>();
    Map<String, TableProcess> cachedTable = new HashMap<>();
    Connection phoenixConn;

    // 初始化操作，建立 hbase的连接，周期性读取配置表信息并存放到内存中
    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        phoenixConn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        getData();
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                getData();
            }
        },5000,5000);
    }


    // 通过连接将 MySQL中的配置表信息包装成对象并存放到内存中
    public void getData(){
        try {
            List<TableProcess> tableProcessesList = MySQLUtil.queryList("select * from table_process", TableProcess.class, true);
            for (TableProcess tableProcess : tableProcessesList) {
                String sourceTable = tableProcess.getSourceTable();
                String operateType = tableProcess.getOperateType();
                String sinkTable = tableProcess.getSinkTable();
                String sinkType = tableProcess.getSinkType();
                String key = sourceTable +":"+operateType;
                cachedTable.put(key,tableProcess);

                // 如果数据去往 hbase方向，并且是以插入的形式新增，尝试加载到缓存中的存在表中，如果不存在，就创建
                if ("hbase".equals(sinkType) && "insert".equals(operateType)){
                    boolean nonExist = existTable.add(sinkTable);
                    if (nonExist){
                        createTable(sinkTable,tableProcess.getSinkColumns(),tableProcess.getSinkPk(),tableProcess.getSinkExtend());
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // 通过拼接的方式创建 sql语句，并在 hbase中创建表
    public void createTable(String tableName,String fields, String pk, String ext){
        if (pk == null){
            pk = "id";
        }
        if (ext == null){
            ext = "";
        }
        StringBuilder createSql = new StringBuilder("create table if not exists " + GmallConfig.HBASE_SCHEMA + "." + tableName + "(");
        String[] colNames = fields.split(",");
        for (int i = 0; i < colNames.length; i++) {
            String colName = colNames[i];
            if (pk.equals(colName)){
                createSql.append(colName).append(" varchar primary key");
            }else {
                createSql.append("info.").append(colName).append(" varchar ");
            }
            if (i<colNames.length-1){
                createSql.append(",");
            }
        }
        createSql.append(")").append(ext);

        try {
            PreparedStatement pstmt = phoenixConn.prepareStatement(createSql.toString());
            pstmt.execute();
            pstmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    public void filterColumns(JSONObject obj, String fields){
        String[] colNames = fields.split(",");
        List<String> cols = Arrays.asList(colNames);
        Set<Map.Entry<String, Object>> entries = obj.entrySet();
        Iterator<Map.Entry<String, Object>> iterator = entries.iterator();
        for (;iterator.hasNext();){
            Map.Entry<String, Object> kv = iterator.next();
            if (!cols.contains(kv.getKey())){
                iterator.remove();
            }
        }
    }

    @Override
    public void processElement(JSONObject obj, Context ctx, Collector<JSONObject> out) throws Exception {
        String table = obj.getString("table");
        String type = obj.getString("type");
        JSONObject data = obj.getJSONObject("data");

        if (type.equals("bootstrap-insert")){
            obj.put("type","insert");
        }
        if (cachedTable!=null && cachedTable.size()>0){
            String key = table +":"+ type;
            TableProcess row = cachedTable.get(key);
            if (row!=null){
                String sinkTable = row.getSinkTable();
                if (row.getSinkColumns()!=null && row.getSinkColumns().length()>0){
                    filterColumns(data,row.getSinkColumns());
                }else {
                    System.out.println("no such key "+ key);
                }
            }
            if (row!=null && TableProcess.SINK_TYPE_HBASE.equals(row.getSinkType())){
                ctx.output(dimTag,obj);
            }else if (row!=null && TableProcess.SINK_TYPE_KAFKA.equals(row.getSinkType())){
                out.collect(obj);
            }
        }
    }
}
