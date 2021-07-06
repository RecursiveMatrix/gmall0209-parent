package com.atguigu.App.practice;


import scala.Int;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class threadtoolutils1 {
   private static ThreadPoolExecutor pool;
   public static ThreadPoolExecutor getInstance(){
       if (pool == null){
           synchronized (threadtoolutils1.class){
               if (pool == null){
                   new ThreadPoolExecutor(4,30,300,TimeUnit.SECONDS,
                           new ArrayBlockingQueue<Runnable>(Integer.MAX_VALUE));
               }
           }
       }
       return pool;
   }
}
