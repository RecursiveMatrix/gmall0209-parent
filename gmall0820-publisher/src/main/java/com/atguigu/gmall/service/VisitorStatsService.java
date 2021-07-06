package com.atguigu.gmall.service;

import com.atguigu.gmall.Bean.VisitorStats;

import java.util.List;

public interface VisitorStatsService {
    public List<VisitorStats> getVisitorStatsByNewFlag(int date);

    public List<VisitorStats> selectVisitorStatsByHour(int date);

    public Long selectPv(int date);

    public Long selectUv(int date);
}
