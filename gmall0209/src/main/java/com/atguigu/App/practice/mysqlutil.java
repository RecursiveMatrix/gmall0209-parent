package com.atguigu.App.practice;

import com.atguigu.bean.TableProcess;
import com.google.common.base.CaseFormat;
import com.google.inject.internal.cglib.reflect.$FastMember;
import lombok.SneakyThrows;
import org.apache.commons.beanutils.BeanUtils;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class mysqlutil {

    @SneakyThrows
    public static <T> List<T> queryList(String sql, Class<T> clazz, Boolean transfer) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;


        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://hadoop201:3306/gmall0820_realtime?characterEncoding=utf-8&useSSL=false",
                    "root",
                    "root");
            ps = conn.prepareStatement(sql);
            rs = ps.executeQuery();
            ResultSetMetaData metaData = rs.getMetaData();
            List<T> resultList = new ArrayList<>();
            while (rs.next()){
                T tableProcess = clazz.newInstance();
                for (int i = 1; i <= metaData.getColumnCount() ; i++) {
                    String columnName = metaData.getColumnName(i);
                    if(transfer){
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
            if(ps!=null){
                ps.close();
            }
            if(conn!=null){
                conn.close();
            }
        }
    }

    public static void main(String[] args) {
        List<TableProcess> tableProcesses = queryList("select * from table_process", TableProcess.class, true);
        for (TableProcess tableProcess : tableProcesses) {
            System.out.println(tableProcess);
        }
    }
}
