package com.bestcoder.lili;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

public class SqlColumnLineageParser extends SqlColumnLineageASTVisitor {

    private String sql;

    public SqlColumnLineageParser(String sql) {
        this.sql = sql;
    }

    public Map<String, Set<String>> parse() {
        SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(this.sql);
        sqlStatement.accept(this);

        Stack<DataSetSource> dataSetSourceStack = this.dataSetSourceStack;
        Map<String,Set<String>> sqlColumnLineage = new HashMap<>(dataSetSourceStack.size());
        for (DataSetSource dataSetSource : dataSetSourceStack) {
            for (Column column : dataSetSource.getColumnList()) {
                for (String table : column.getSourceTable()) {
                    Set<String> columnSet = sqlColumnLineage.get(table);
                    if(columnSet == null){
                        columnSet = new HashSet<>();
                        sqlColumnLineage.put(table, columnSet);
                    }
                    columnSet.add(column.getName());
                }
            }
        }
        return sqlColumnLineage;
    }
}
