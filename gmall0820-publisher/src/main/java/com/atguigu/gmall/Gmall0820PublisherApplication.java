package com.atguigu.gmall;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication

// 为指定接口创建实现类
@MapperScan(basePackages = "com.atguigu.gmall.mapper")
public class Gmall0820PublisherApplication {

    public static void main(String[] args) {
        SpringApplication.run(Gmall0820PublisherApplication.class, args);
    }

}
