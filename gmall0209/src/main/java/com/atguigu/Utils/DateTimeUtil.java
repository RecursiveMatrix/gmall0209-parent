package com.atguigu.Utils;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class DateTimeUtil {
    // 保证线程安全问题
    public final static DateTimeFormatter formattor = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    public static String toYMDhms(Date date){
        LocalDateTime localDateTime = LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
        return formattor.format(localDateTime);
    }
    public static Long toTs(String YmDHms){
        LocalDateTime localDateTime = LocalDateTime.parse(YmDHms, formattor);
        long ts = localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
        return ts;
    }
}
