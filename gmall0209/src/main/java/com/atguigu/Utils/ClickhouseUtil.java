package com.atguigu.Utils;

import com.atguigu.Common.GmallConfig;
import com.atguigu.bean.TransientSink;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;


// 功能：把当前流中的数据保存到 clickhouse，是一个sink操作，单独引入一个依赖
// 针对所有 jdbc规范的数据库都提供了 sink方法

public class ClickhouseUtil {

    // 该方法获取向 clickhouse中写入的 sink方法
    public static <T>SinkFunction getJdbcSink(String sql){
        // JdbcSink来自与依赖，能够向 jdbc协议支持的数据库发送数据
        /* 该方法需要四个参数：
            1.执行的 sql语句；例如 insert into table xxx values()
            2.执行的写入操作，如何把值赋值给 sql；
            3.设置批次大小写入，给当前属性赋值，这里用到了构建者设计模式
            4.给链接的相关属性赋值，也是构造者数据模式；
            最终返回的是 sinkFunction
         */
        SinkFunction<T> sink = JdbcSink.<T>sink(
                sql,
                // T范型表示流中的数据类型，由于写入不同表需要是不同的类型
                // 通过反射获取类的信息，用来应付不同类型的数据
                // 把流中的数据内容对应给 preparedStatement赋值
                /*
                如果pojo类中由希望过滤掉不传入数据库的字段，可以加 @Transient标记，该属性不会序列化
                 */

                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, T obj) throws SQLException {
                        // 获取当前类中所有的属性
                        Field[] fields = obj.getClass().getDeclaredFields();
                        int skipOffset = 0;
                        for (int i = 0; i < fields.length; i++) {
                            Field field = fields[i];
                            // 先获取注解，如果有 @TransientSink标记，则跳过该属性；
                            TransientSink transientSink = field.getAnnotation(TransientSink.class);
                            if (transientSink != null) {
                                skipOffset++;
                                continue;
                            }
                            // 如果是私有的，设置私有属性可访问
                            field.setAccessible(true);
                            // 获取属性值对象
                            try {
                                Object attributeObj = field.get(obj);
                                // index为 0的对象赋给 1位置的pstmt
                                preparedStatement.setObject(i + 1 - skipOffset, attributeObj);
                            } catch (IllegalAccessException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                },
                new JdbcExecutionOptions.Builder().withBatchSize(2).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .withDriverName("ru.yandex.clickhouse.ClickHouseDriver").build()
        );
        return sink;
    }
}
