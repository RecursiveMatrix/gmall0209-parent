package com.atguigu.App.practice;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.Common.GmallConfig;
import org.apache.commons.beanutils.BeanUtils;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class phoenixutil {
    private static Connection conn = null;

    public static void init(){
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
            conn.setSchema(GmallConfig.HBASE_SCHEMA);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    public static <T> List<T> queryList(String sql, Class<T> clazz) throws SQLException {
        PreparedStatement ps = null;
        ResultSet rs = null;
        if(conn == null){
            init();
        }
        try {
            ps = conn.prepareStatement(sql);
            rs = ps.executeQuery();
            ResultSetMetaData metaData = rs.getMetaData();
            List<T> resultList = new ArrayList<>();
            while (rs.next()){
                T jsonObj = clazz.newInstance();
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    BeanUtils.setProperty(jsonObj,metaData.getColumnName(i),rs.getObject(i));
                }
                resultList.add(jsonObj);
            }
            return resultList;

        } catch (SQLException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
            throw new RuntimeException("no result");
        }finally {
            if(rs!=null){
                rs.close();
            }
            if(ps!=null){
                ps.close();
            }
            if(conn!=null){
                conn.close();
            }
        }
    }


    public static void main(String[] args) {
        List<JSONObject> jsonObjects = null;
        try {
            jsonObjects = queryList("select * from dim_base_trademark", JSONObject.class);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println(jsonObjects);
    }
}
