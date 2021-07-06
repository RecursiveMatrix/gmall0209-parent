package com.atguigu.gmall.service;


import com.atguigu.gmall.Bean.KeywordStats;
import org.apache.ibatis.annotations.Param;

import java.util.List;

public interface KeywordStatsService {

    public List<KeywordStats> getKeywordStats(int date,int limit);
}
