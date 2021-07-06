package com.atguigu.gmall.mapper;

import com.atguigu.gmall.Bean.KeywordStats;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface KeywordStatsMapper {

    @Select("select keyword, sum(ct*\n" +
            "                    multiIf(\n" +
            "source='SEARCH',10,\n" +
            "source='ORDER',5,\n" +
            "source='CART',2,\n" +
            "source='CLICK',1,0)) ct\n" +
            "from\n" +
            "keyword_stats_0820\n" +
            "where toYYYYMMDD(stt)=#{date}\n" +
            "group by keyword\n" +
            "order by ct desc\n" +
            "limit #{limit};")

    public List<KeywordStats> selectWordStats(@Param("date") int date, @Param("limit") int limit);
}
