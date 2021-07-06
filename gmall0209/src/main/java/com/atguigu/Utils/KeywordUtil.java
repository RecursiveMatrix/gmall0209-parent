package com.atguigu.Utils;

// 基于IK的分词器

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class KeywordUtil {

    // 将字符串进行分词，分词后的结果放到一个集合中返回
    public static List<String> analyze(String text){
        List<String> wordList = new ArrayList<>();

        // 字符转化为输入流
        StringReader reader = new StringReader(text);
        IKSegmenter ikSegmenter = new IKSegmenter(reader,true);

        // 分词后的一个单词对象
        Lexeme lexme = null;

        while (true){
            try {
                lexme = ikSegmenter.next();
                if (lexme!=null){
                    String word = lexme.getLexemeText();
                    wordList.add(word);
                }else {
                    break;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return wordList;
    }

    public static void main(String[] args) {
        String text = "Apple iPhoneXSMax (A2104) 256GB 深空灰色 移动联通电信4G手机 双卡双待";
        System.out.println(KeywordUtil.analyze(text));
    }
}
