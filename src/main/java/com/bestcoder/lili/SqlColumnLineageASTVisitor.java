package com.bestcoder.lili;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLAllColumnExpr;
import com.alibaba.druid.sql.ast.expr.SQLArrayExpr;
import com.alibaba.druid.sql.ast.expr.SQLBetweenExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLCaseExpr;
import com.alibaba.druid.sql.ast.expr.SQLCastExpr;
import com.alibaba.druid.sql.ast.expr.SQLContainsExpr;
import com.alibaba.druid.sql.ast.expr.SQLDateExpr;
import com.alibaba.druid.sql.ast.expr.SQLFlashbackExpr;
import com.alibaba.druid.sql.ast.expr.SQLGroupingSetExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLInListExpr;
import com.alibaba.druid.sql.ast.expr.SQLInSubQueryExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntervalExpr;
import com.alibaba.druid.sql.ast.expr.SQLListExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.expr.SQLSizeExpr;
import com.alibaba.druid.sql.ast.expr.SQLUnaryExpr;
import com.alibaba.druid.sql.ast.expr.SQLValuesExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSubqueryTableSource;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.ast.statement.SQLUnionQueryTableSource;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Stack;

public class SqlColumnLineageASTVisitor extends MySqlASTVisitorAdapter {

    @Data
    public class Column{
        private String name;
        private String owner;
        private String alias;
        private Set<String> sourceTable = new HashSet<>();
    }

    @Data
    public class DataSetSource {
        private String alias;
        private List<Column> columnList = new ArrayList<>();

        public DataSetSource(String alias){
            this.alias = alias;
        }
        public DataSetSource(){
            this(null);
        }
    }

    protected Stack<DataSetSource> dataSetSourceStack = new Stack<>();
    private static final String ALL_COLUMN_TAG = "*";
    private static final String DATA_SET_SOURCE_TAG = "DATA_SET_SOURCE";

    @Override
    public boolean visit(SQLExprTableSource sqlExprTableSource) {
        String dataSetAlias = sqlExprTableSource.getAlias();
        String dataSetName = sqlExprTableSource.getName().getSimpleName();
        DataSetSource top = dataSetSourceStack.peek();

        for (Column column : top.getColumnList()) {
            String owner = column.getOwner();
            if(StringUtils.isBlank(owner) || owner.equals(dataSetAlias)){
                column.getSourceTable().add(dataSetName);
            }
        }
        return true;
    }

    @Override
    public boolean visit(SQLSubqueryTableSource sqlSubqueryTableSource) {
        pushDataSetSource(sqlSubqueryTableSource);
        return true;
    }

    @Override
    public void endVisit(SQLSubqueryTableSource sqlSubqueryTableSource) {
        popDataSetSource(sqlSubqueryTableSource);
    }

    @Override
    public boolean visit(SQLUnionQueryTableSource sqlUnionQueryTableSource) {
        pushDataSetSource(sqlUnionQueryTableSource);
        return true;
    }

    private void pushDataSetSource(SQLTableSource sqlTableSource) {
        DataSetSource dataSetSource = new DataSetSource(sqlTableSource.getAlias());
        dataSetSourceStack.push(dataSetSource);
        sqlTableSource.putAttribute(DATA_SET_SOURCE_TAG, dataSetSource);
    }

    private void popDataSetSource(SQLTableSource sqlTableSource) {
        DataSetSource dataSetSource = (DataSetSource)sqlTableSource.getAttribute(DATA_SET_SOURCE_TAG);
        DataSetSource top = null;
        while ((top = dataSetSourceStack.peek()) != dataSetSource) {
            dataSetSourceStack.pop();
            for (Column column : top.getColumnList()) {
                dataSetSource.getColumnList().add(column);
            }
        }
    }

    private void pushDataSetSource(MySqlSelectQueryBlock sqlSelectQueryBlock) {
        DataSetSource dataSetSource = new DataSetSource();
        dataSetSourceStack.push(dataSetSource);
        sqlSelectQueryBlock.putAttribute(DATA_SET_SOURCE_TAG, dataSetSource);
    }

    private void popDataSetSource(MySqlSelectQueryBlock sqlSelectQueryBlock) {
        DataSetSource dataSetSource = (DataSetSource)sqlSelectQueryBlock.getAttribute(DATA_SET_SOURCE_TAG);
        while (dataSetSourceStack.peek() != dataSetSource) {
            final DataSetSource top = dataSetSourceStack.pop();
            List<Column> oldColumnList = dataSetSource.getColumnList();
            List<Column> newColumnList = new ArrayList<>(oldColumnList);
            dataSetSource.setColumnList(newColumnList);

            for (Column oldColumn : oldColumnList) {
                String oldColumnName = oldColumn.getName();
                String owner = oldColumn.getOwner();
                boolean noOwner = StringUtils.isBlank(owner);
                boolean allColumn = ALL_COLUMN_TAG.equals(oldColumnName);

                if (noOwner && allColumn) {
                    newColumnList.remove(oldColumn);
                    newColumnList.addAll(top.getColumnList());
                    break;
                } else if (!noOwner && allColumn) {
                    if (owner.equals(top.getAlias())) {
                        newColumnList.remove(oldColumn);
                        newColumnList.addAll(top.getColumnList());
                    }
                } else if(noOwner || owner.equals(top.getAlias())){
                    for (Column newColumn : top.getColumnList()) {
                        String newColumnName = newColumn.getName();
                        String newColumnAlias = newColumn.getAlias();

                        if (ALL_COLUMN_TAG.equals(newColumnName)
                                || oldColumnName.equals(newColumnName)) {
                            oldColumn.getSourceTable().addAll(newColumn.getSourceTable());
                        }else if(oldColumnName.equals(newColumnAlias)){
                            newColumnList.remove(oldColumn);
                            newColumnList.add(newColumn);
                        }
                    }
                }
            }
        }
    }

    @Override
    public void endVisit(SQLUnionQueryTableSource sqlUnionQueryTableSource) {
        popDataSetSource(sqlUnionQueryTableSource);
    }

    @Override
    public boolean visit(MySqlSelectQueryBlock mySqlSelectQueryBlock) {
        pushDataSetSource(mySqlSelectQueryBlock);
        return true;
    }

    @Override
    public void endVisit(MySqlSelectQueryBlock mySqlSelectQueryBlock) {
        popDataSetSource(mySqlSelectQueryBlock);
    }

    private void visitSQLExpr(String selectItemAlias, SQLExpr expr) {
        if (expr instanceof SQLIdentifierExpr) {
            SQLIdentifierExpr sqlIdentifierExpr = (SQLIdentifierExpr)expr;
            pushColumn(selectItemAlias, sqlIdentifierExpr.getName(), null);
        } else if (expr instanceof SQLPropertyExpr) {
            SQLPropertyExpr sqlPropertyExpr = (SQLPropertyExpr)expr;
            pushColumn(selectItemAlias, sqlPropertyExpr.getName(), sqlPropertyExpr.getOwnernName());
        } else if (expr instanceof SQLAllColumnExpr) {
            pushColumn(selectItemAlias, ALL_COLUMN_TAG, null);
        } else if (expr instanceof SQLMethodInvokeExpr) {
            List<SQLExpr> arguments = ((SQLMethodInvokeExpr)expr).getArguments();
            for (SQLExpr argument : arguments) {
                visitSQLExpr(selectItemAlias, argument);
            }
        }else if(expr instanceof SQLBinaryOpExpr){
            SQLBinaryOpExpr sqlBinaryOpExpr = (SQLBinaryOpExpr)expr;
            visitSQLExpr(selectItemAlias, sqlBinaryOpExpr.getLeft());
            visitSQLExpr(selectItemAlias, sqlBinaryOpExpr.getRight());
        }else if(expr instanceof SQLCaseExpr){
            SQLCaseExpr sqlCaseExpr = (SQLCaseExpr) expr;
            for (SQLCaseExpr.Item item : sqlCaseExpr.getItems()) {
                visitSQLExpr(selectItemAlias, item.getConditionExpr());
                visitSQLExpr(selectItemAlias, item.getValueExpr());
            }
            visitSQLExpr(selectItemAlias, sqlCaseExpr.getElseExpr());
        }else if(expr instanceof SQLBetweenExpr){
            SQLBetweenExpr sqlBetweenExpr = (SQLBetweenExpr) expr;
            visitSQLExpr(selectItemAlias, sqlBetweenExpr.getTestExpr());
            visitSQLExpr(selectItemAlias, sqlBetweenExpr.getBeginExpr());
            visitSQLExpr(selectItemAlias, sqlBetweenExpr.getEndExpr());
        }else if(expr instanceof SQLArrayExpr){
            SQLArrayExpr sqlArrayExpr = (SQLArrayExpr)expr;
            visitSQLExpr(selectItemAlias, sqlArrayExpr.getExpr());
        }else if(expr instanceof SQLArrayExpr){
            SQLArrayExpr sqlArrayExpr = (SQLArrayExpr)expr;
            visitSQLExpr(selectItemAlias, sqlArrayExpr.getExpr());
        }else if(expr instanceof SQLCastExpr){
            SQLCastExpr sqlCastExpr = (SQLCastExpr)expr;
            visitSQLExpr(selectItemAlias, sqlCastExpr.getExpr());
        }else if(expr instanceof SQLContainsExpr){
            SQLContainsExpr sqlContainsExpr = (SQLContainsExpr)expr;
            visitSQLExpr(selectItemAlias, sqlContainsExpr.getExpr());
            for (SQLExpr sqlExpr : sqlContainsExpr.getTargetList()) {
                visitSQLExpr(selectItemAlias, sqlExpr);
            }
        }else if(expr instanceof SQLDateExpr){
            SQLDateExpr sqlDateExpr = (SQLDateExpr)expr;
            visitSQLExpr(selectItemAlias, sqlDateExpr.getLiteral());
        }else if(expr instanceof SQLFlashbackExpr){
            SQLFlashbackExpr sqlFlashbackExpr = (SQLFlashbackExpr)expr;
            visitSQLExpr(selectItemAlias, sqlFlashbackExpr.getExpr());
        }else if(expr instanceof SQLGroupingSetExpr){
            SQLGroupingSetExpr sqlGroupingSetExpr = (SQLGroupingSetExpr)expr;
            for (SQLExpr parameter : sqlGroupingSetExpr.getParameters()) {
                visitSQLExpr(selectItemAlias, parameter);
            }
        }else if(expr instanceof SQLInListExpr){
            SQLInListExpr sqlInListExpr = (SQLInListExpr)expr;
            visitSQLExpr(selectItemAlias, sqlInListExpr.getExpr());
            for (SQLExpr sqlExpr : sqlInListExpr.getTargetList()) {
                visitSQLExpr(selectItemAlias, sqlExpr);
            }
        }else if(expr instanceof SQLInSubQueryExpr){
            SQLInSubQueryExpr sqlInSubQueryExpr = (SQLInSubQueryExpr)expr;
            visitSQLExpr(selectItemAlias, sqlInSubQueryExpr.getExpr());
        }else if(expr instanceof SQLIntervalExpr){
            SQLIntervalExpr sqlIntervalExpr = (SQLIntervalExpr)expr;
            visitSQLExpr(selectItemAlias, sqlIntervalExpr.getValue());
        }else if(expr instanceof SQLListExpr){
            SQLListExpr sqlListExpr = (SQLListExpr)expr;
            for (SQLExpr item : sqlListExpr.getItems()) {
                visitSQLExpr(selectItemAlias, item);
            }
        }else if(expr instanceof SQLSizeExpr){
            SQLSizeExpr sqlSizeExpr = (SQLSizeExpr)expr;
            visitSQLExpr(selectItemAlias, sqlSizeExpr.getValue());
        }else if(expr instanceof SQLUnaryExpr){
            SQLUnaryExpr sqlUnaryExpr = (SQLUnaryExpr)expr;
            visitSQLExpr(selectItemAlias, sqlUnaryExpr.getExpr());
        }else if(expr instanceof SQLValuesExpr){
            SQLValuesExpr sqlValuesExpr = (SQLValuesExpr)expr;
            for (SQLListExpr value : sqlValuesExpr.getValues()) {
                visitSQLExpr(selectItemAlias, value);
            }
        }
    }

    @Override
    public boolean visit(SQLSelectItem selectItem) {
        String selectItemAlias = selectItem.getAlias();
        SQLExpr expr = selectItem.getExpr();
        visitSQLExpr(selectItemAlias, expr);
        return true;
    }

    private void pushColumn(String selectItemAlias, String name, String owner) {
        Column column = new Column();
        column.setAlias(selectItemAlias);
        column.setName(name);
        column.setOwner(owner);
        dataSetSourceStack.peek().getColumnList().add(column);
    }
}
