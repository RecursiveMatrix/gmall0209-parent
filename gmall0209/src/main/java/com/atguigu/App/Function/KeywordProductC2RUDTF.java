package com.atguigu.App.Function;

import com.atguigu.Common.GmallConstant;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;


@FunctionHint(output = @DataTypeHint("ROW<ct BIGINT, source String>"))
public class KeywordProductC2RUDTF extends TableFunction<Row> {

    public void eval(Long clickCt,Long cartCt,Long orderCt){
        if (clickCt>0){
            Row rowClick = new Row(2);
            rowClick.setField(0,clickCt);
            rowClick.setField(1, GmallConstant.KEYWORD_CLICK);
            collect(rowClick);
        }
        if (cartCt>0){
            Row rowClick = new Row(2);
            rowClick.setField(0,clickCt);
            rowClick.setField(1, GmallConstant.KEYWORD_CART);
            collect(rowClick);
        }
        if (orderCt>0){
            Row rowClick = new Row(2);
            rowClick.setField(0,clickCt);
            rowClick.setField(1, GmallConstant.KEYWORD_ORDER);
            collect(rowClick);
        }
    }
}
