package com.atguigu.gmall.mapper;

import com.atguigu.gmall.Bean.ProductStats;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;
import java.util.List;

public interface ProductStatsMapper {

    // 通过注解来实现声明的抽象方法； #{}的方式引入参数
    @Select("select sum(order_amount) order_amount  " +
            "from product_stats_0820 where toYYYYMMDD(stt)=#{date}")
    // 获取某一天的商品交易额
    BigDecimal getGMV(int date);

    @Select("select spu_id,spu_name,sum(order_amount) order_amount,sum(product_stats_0820.order_ct) order_ct\n" +
            "from product_stats_0820\n" +
            "where toYYYYMMDD(stt) = #{date}\n" +
            "group by spu_id, spu_name\n" +
            "having order_amount>0\n" +
            "order by order_amount desc limit #{limit};")
    public List<ProductStats> getProductStatsGroupBySpu(@Param("date") int date, @Param("limit") int limit);

    //统计某天不同类别商品交易额排名
    @Select("select category3_id,category3_name,sum(order_amount) order_amount " +
            "from product_stats_0820 " +
            "where toYYYYMMDD(stt)=#{date} group by category3_id,category3_name " +
            "having order_amount>0  order by  order_amount desc limit #{limit}")
    public List<ProductStats> getProductStatsGroupByCategory3(@Param("date")int date , @Param("limit") int limit);

    //统计某天不同品牌商品交易额排名
    @Select("select tm_id,tm_name,sum(order_amount) order_amount " +
            "from product_stats_0820 " +
            "where toYYYYMMDD(stt)=#{date} group by tm_id,tm_name " +
            "having order_amount>0  order by  order_amount  desc limit #{limit} ")
    public List<ProductStats> getProductStatsByTrademark(@Param("date")int date,  @Param("limit") int limit);

}
