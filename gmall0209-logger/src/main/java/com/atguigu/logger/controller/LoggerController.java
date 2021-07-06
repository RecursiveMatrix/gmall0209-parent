package com.atguigu.logger.controller;



// 接收模拟器生成的数据并对其处理

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


// controller 将对象创建交给容器，放回string，默认把结果当作跳转页面处理
@RestController // 方法返回object，底层转化为json格式的字符串进行对应
public class LoggerController {

    @RequestMapping("/applog")
    public String applog(@RequestBody String jsonData){
        System.out.println(jsonData);
        return jsonData;

    }
}
