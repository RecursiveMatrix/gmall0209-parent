package com.atguigu.gmall.mapper;

import com.atguigu.gmall.Bean.VisitorStats;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface VisitorStatsMapper {


    @Select("select is_new,sum(uv_ct) uv_ct,sum(pv_ct) pv_ct,sum(sv_ct) sv_ct, sum(uj_ct) uj_ct,sum(dur_sum) dur_sum " +
            "from visitor_stats_0820 " +
            "where toYYYYMMDD(stt)= #{date} group by is_new")
    public List<VisitorStats> selectVisitorStatsByNewFlag(int date);


    @Select("select sum(if(is_new='1', visitor_stats_0820.uv_ct,0)) new_uv,toHour(stt) hr," +
            "sum(visitor_stats_0820.uv_ct) uv_ct, sum(pv_ct) pv_ct, sum(uj_ct) uj_ct  " +
            "from visitor_stats_0820 where toYYYYMMDD(stt)=#{date} group by toHour(stt)")
    public List<VisitorStats> selectVisitorStatsByHour(int date);

    @Select("select count(pv_ct) pv_ct from visitor_stats_0820 " +
            "where toYYYYMMDD(stt)=#{date}  ")
    public Long selectPv(int date);

    @Select("select count(uv_ct) uv_ct from visitor_stats_0820 " +
            "where toYYYYMMDD(stt)=#{date}  ")
    public Long selectUv(int date);


}
