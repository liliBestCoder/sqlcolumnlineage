package com.bestcoder.lili;

import java.util.Map;
import java.util.Set;

public class SqlColumnLineageParseUtil {

    public static Map<String, Set<String>> parse(String sql){
        SqlColumnLineageParser sqlColumnLineageParser = new SqlColumnLineageParser(sql);
        return sqlColumnLineageParser.parse();
    }
}
