package com.atguigu.gmall.Bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class KeywordStats {

    private String stt;
    private String edt;
    private String keyword;
    private String source;
    private Long ct;
    private String ts;

}
