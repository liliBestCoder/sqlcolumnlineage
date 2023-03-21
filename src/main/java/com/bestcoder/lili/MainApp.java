package com.bestcoder.lili;

import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.Set;

public class MainApp {
    
    public static void main(String... args) throws Exception {
        int length = args.length;
        if(length < 2){
            throw new RuntimeException("params invalid.");
        }

        String command = args[0];
        if(StringUtils.isBlank(command) && "-s".equals(command)){
            throw new RuntimeException("command invalid.");
        }

        String sql = args[1];
        if(StringUtils.isBlank(sql)){
            throw new RuntimeException("sql cannot be empty.");
        }

        Map<String, Set<String>> sqlColumnLineageMapping = SqlColumnLineageParseUtil.parse(sql);
        System.out.println(sqlColumnLineageMapping);
    }

}

