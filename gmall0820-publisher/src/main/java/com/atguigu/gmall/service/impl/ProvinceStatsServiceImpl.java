package com.atguigu.gmall.service.impl;

import com.atguigu.gmall.Bean.ProvinceStats;
import com.atguigu.gmall.mapper.ProvinceStatsMapper;
import com.atguigu.gmall.service.ProvinceStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class ProvinceStatsServiceImpl implements ProvinceStatsService {

    @Autowired
    ProvinceStatsMapper provinceStatsMapper;


    @Override
    public List<ProvinceStats> getProvinceStats(int date) {
        return provinceStatsMapper.selectProvinceStats(date);
    }
}
