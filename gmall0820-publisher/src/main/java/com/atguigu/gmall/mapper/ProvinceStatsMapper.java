package com.atguigu.gmall.mapper;

import com.atguigu.gmall.Bean.ProvinceStats;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface ProvinceStatsMapper {

    // 按照地区查询交易额
    @Select("select province_name,sum(order_amount) order_amount " +
            "from province_stats_0820 " +
            "where toYYYYMMDD(stt) = #{date} " +
            "group by province_id,province_name")
    public List<ProvinceStats> selectProvinceStats(int date);

}

