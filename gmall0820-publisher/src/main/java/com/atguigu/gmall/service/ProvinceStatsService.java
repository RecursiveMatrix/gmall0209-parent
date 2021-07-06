package com.atguigu.gmall.service;

import com.atguigu.gmall.Bean.ProvinceStats;

import java.util.List;

public interface ProvinceStatsService {
    public List<ProvinceStats> getProvinceStats(int date);
}
