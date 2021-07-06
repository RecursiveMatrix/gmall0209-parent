package com.atguigu.Utils;

import com.atguigu.bean.TableProcess;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class MySQLUtil {

    // 主要实现方法，通过给定的 sql， 包装类型，是否驼峰转化字段名
    public static <T> List<T> queryList(String sql, Class<T> clazz, Boolean underscoreToCamel) throws SQLException {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        // 建立 sql 连接：1.注册驱动 2.获取连接  3.包装pstmt   4.执行语句  5.获取结果  6.关闭资源
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://hadoop201:3306/gmall0820_realtime?characterEncoding=utf-8&useSSL=false",
                    "root",
                    "root");
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            ResultSetMetaData metaData = rs.getMetaData();

            List<T> resultList = new ArrayList<>();
            while (rs.next()){
                T tableProcess = clazz.newInstance();
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    String columnName = metaData.getColumnName(i);
                    if(underscoreToCamel){
                        columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                    }
                    BeanUtils.setProperty(tableProcess,columnName,rs.getObject(i));
                }
                resultList.add(tableProcess);
            }
            return resultList;

        } catch (ClassNotFoundException | SQLException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
            throw new RuntimeException("查询数据失败！");
        }finally {
            if(rs!=null){
                rs.close();
            }
            if(pstmt!=null){
                pstmt.close();
            }
            if(conn!=null){
                conn.close();
            }
        }
    }


    public static void main(String[] args) throws SQLException {
        List<TableProcess> tableProcesses = queryList("select * from table_process", TableProcess.class, true);
        for (TableProcess tableProcess : tableProcesses) {
            System.out.println(tableProcess);
        }
    }
}
