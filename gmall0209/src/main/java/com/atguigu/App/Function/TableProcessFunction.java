package com.atguigu.App.Function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.Common.GmallConfig;
import com.atguigu.Utils.MySQLUtil;
import com.atguigu.bean.TableProcess;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.*;
import java.util.*;


/*
    该方法的目的是处理数据流中的 Json对象的信息，把对应的维度表数据放入侧输出流
    1. 启动方法：初始化 phoenix链接，调用跟新配置表方法，并之后定时周期性执行；
    2. 跟新配置表方法：包装查询结果，转化成为特定的类型的数组对象；
    3. 检查配置表方法：拼接 sql命令，根据配置表信息建立对应 kafka主题表；
    4. 过滤字段方法：根据配置表信息，筛选指定的字段


 */
public class TableProcessFunction extends ProcessFunction<JSONObject,JSONObject> {

    // TODO 1.该方法类的构造：需要传入维度侧输出流的标签
    OutputTag<JSONObject> hbaseTag;
    public TableProcessFunction(OutputTag<JSONObject> hbaseTag){
        this.hbaseTag = hbaseTag;
    }

    // 声明 hbase数据库链接，一个 hashmap 用来存放表的具体信息(), haseSet 用来记录当前写回 kafka中的表
    Connection phoenixConn = null;
    Map<String,TableProcess> cacheMap = new HashMap<>();
    Set<String> existedTables = new HashSet<>();

//    ResultSet rs = null;
//    PreparedStatement pstmt = null;
//    List<TableProcess> resultList = new ArrayList<TableProcess>();

    // TODO 2.初始化方法中完成对 phoenix链接的获取，以及周期性调用数据获取方法
    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        phoenixConn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        initTable();
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                initTable();
            }
        },5000,5000);

    }

    // TODO 3.通过之前定义的 MySqlUtil中方法获取 hbase中的表信息，并包装成指定的 pojos类
    private void initTable(){
        try {
            List<TableProcess> tableProcessesList = MySQLUtil.queryList("select * from table_process", TableProcess.class, true);
            for (TableProcess tableProcess : tableProcessesList) {
                String sourceTable = tableProcess.getSourceTable();
                String operateType = tableProcess.getOperateType();
                String sinkTable = tableProcess.getSinkTable();
                String sinkType = tableProcess.getSinkType();
                String key = sourceTable+":"+operateType;
                cacheMap.put(key,tableProcess);

                if("hbase".equals(sinkType) && "insert".equals(operateType)){
                    boolean nonExist = existedTables.add(sinkTable);
                    if(nonExist){
                        checkTable(sinkTable,tableProcess.getSinkColumns(),tableProcess.getSinkPk(),tableProcess.getSinkExtend());
                    }
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // TODO 4.根据表名，字段名，主键，附属信息，创建新的事实表：拼接 sql 字符串为重点
    private void checkTable(String tableName, String fields, String pk, String ext){
        if(pk == null){
            pk = "id";
        }
        if(ext == null){
            ext = "";
        }
        StringBuilder createSql = new StringBuilder("create table if not exists " + GmallConfig.HBASE_SCHEMA + "." + tableName + "(");
        String[] colNames = fields.split(",");
        for (int i = 0; i < colNames.length; i++) {
            String colName = colNames[i];
            if(pk.equals(colName)){
                createSql.append(colName).append(" varchar ").append("primary key ");
            }else {
                createSql.append("info.").append(colName).append(" varchar ");
            }
            if(i<colNames.length-1){
                createSql.append(",");
            }
        }
        createSql.append(")");
        createSql.append(ext);

        try {
            PreparedStatement pstmt = phoenixConn.prepareStatement(createSql.toString());
            pstmt.execute();
            pstmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    // TODO 5.传入原始数据对象和配置表中自定义字段，对数据进行ETL
    private void filterColumns(JSONObject sourceData, String pk){
        String[] colNames = StringUtils.split(pk, ",");;
        List<String> colCollection = Arrays.asList(colNames);
        Set<Map.Entry<String, Object>> entries = sourceData.entrySet();
        Iterator<Map.Entry<String, Object>> iterator = entries.iterator();
        // 该处循环为一个 json对象的所有 kv对，如果该 kv对的 key 不存在配置表中，则删除
        for(;iterator.hasNext();){
            Map.Entry<String, Object> kvSet = iterator.next();
            if(!colCollection.contains(kvSet.getKey())){
                iterator.remove();
            }
        }
    }


    // TODO 6.根据流中对象的信息，确定分流

    @Override
    public void processElement(JSONObject jsonObj, Context ctx, Collector<JSONObject> out) throws Exception {
        String table = jsonObj.getString("table");
        String type = jsonObj.getString("type");
        JSONObject data = jsonObj.getJSONObject("data");

        // maxwell 附加的insert信息实际上标记为 bootstrap-insert，需要转化为insert
        if(type.equals("bootstrap-insert")){
            type = "insert";
            jsonObj.put("type",type);
        }

        if(cacheMap != null && cacheMap.size()>0){
            String key = table+":"+type;
            TableProcess tableProcess = cacheMap.get(key);
            if(tableProcess!=null){
                String sinkTable = tableProcess.getSinkTable();
                jsonObj.put("sink_table",sinkTable);
                if(tableProcess.getSinkColumns()!=null && tableProcess.getSinkColumns().length()>0){
                    filterColumns(data,tableProcess.getSinkColumns());
                }else {
                    System.out.println("no such key"+ key);
                }
            }
            if(tableProcess!=null && TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())){
                ctx.output(hbaseTag,jsonObj);
            }else if(tableProcess!=null && TableProcess.SINK_TYPE_KAFKA.equals(tableProcess.getSinkType())){
                out.collect(jsonObj);
            }
        }
    }
}
