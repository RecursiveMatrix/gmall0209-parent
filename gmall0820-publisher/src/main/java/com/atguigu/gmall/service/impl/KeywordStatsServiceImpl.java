package com.atguigu.gmall.service.impl;

import com.atguigu.gmall.Bean.KeywordStats;
import com.atguigu.gmall.mapper.KeywordStatsMapper;
import com.atguigu.gmall.service.KeywordStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;


@Service
public class KeywordStatsServiceImpl implements KeywordStatsService {

    @Autowired
    KeywordStatsMapper keywordStatsMapper;
    @Override
    public List<KeywordStats> getKeywordStats(int date, int limit) {
        return keywordStatsMapper.selectWordStats(date,limit);
    }
}
