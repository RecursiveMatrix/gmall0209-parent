package com.atguigu.Utils;


import com.alibaba.fastjson.JSONObject;
import com.atguigu.Common.GmallConfig;
import org.apache.commons.beanutils.BeanUtils;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

// 根据给定的查询 sql语句，和配置表的类型    ====>   从 hbase中查询维度数据并返回结果
public class PhoenixUtil {
    private static Connection conn = null;

    public static void querryInit(){
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
            conn.setSchema(GmallConfig.HBASE_SCHEMA);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    public static <T>List<T> queryList(String sql,Class<T> clazz){
        if(conn == null){
            querryInit();
        }
        List<T> resultList = new ArrayList<>();
        PreparedStatement pstmt;

        try {
            pstmt = conn.prepareStatement(sql);
            ResultSet rs = pstmt.executeQuery();
            ResultSetMetaData metaData = rs.getMetaData();
            while (rs.next()){
                T row = clazz.newInstance();
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    BeanUtils.setProperty(row,metaData.getColumnName(i),rs.getObject(i));
                }
                resultList.add(row);
            }
            pstmt.close();


        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultList;
    }

    public static void main(String[] args) {
        List<JSONObject> objList = queryList("select * from dim_base_trademark where id ='11'", JSONObject.class);
        System.out.println(objList);
    }
}

