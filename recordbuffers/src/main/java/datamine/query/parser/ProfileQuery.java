package datamine.query.parser;

import datamine.query.functions.*;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import datamine.query.data.Column;
import datamine.query.data.Operation;
import datamine.query.data.Value;
import datamine.query.data.ValueUtils;
import datamine.storage.api.RecordMetadataInterface;
import datamine.storage.idl.Field;
import datamine.storage.idl.generator.metadata.GetAllMetadataEnumClasses;
import datamine.storage.idl.generator.metadata.GetAllMetadataEnumClasses2;
import datamine.storage.idl.type.CollectionFieldType;
import datamine.storage.idl.type.FieldType;
import datamine.storage.idl.type.FieldTypeFactory;
import datamine.storage.idl.type.GroupFieldType;
import datamine.storage.idl.type.PrimitiveType;
import datamine.storage.recordbuffers.RecordBufferMeta;

public class ProfileQuery extends PQLBaseListener {

    public static final Logger LOG = LoggerFactory.getLogger(ProfileQuery.class);
    private static final String TIMESTAMP_FIELD_SIMPLE_NAME = "event_time";

    // Input
    private final String metadataPath;
    private final List<String> metadataPathList;

    // Members for all queries
    Map<String, RecordBufferMeta> tableMetadataMap = new HashMap<>();

    // Members for each query
    public static class ParsingResult implements Serializable {

        final String query;
        public ParsingResult(final String query) {
            this.query = query;
        }

        String mainTable = null;
        Map<String, Field> baseFieldMap = new HashMap<>();
        List<Column> baseColumns = new ArrayList<>();
        Map<String, Field> nestedFuncFieldMap = new HashMap<>();
        List<Column> nestedFuncColumns = new ArrayList<>();
        Map<String, Column> mapColumnMap = new LinkedHashMap<>();
        List<Column> selectColumnList = new ArrayList<>();
        Map<String, RecordBufferMeta> tableColumnMetadataMap = new HashMap<>();

        Set<String> whereTables = new HashSet<>();
        List<Long> timeList = new ArrayList<>();
        String startDateTxt = null;
        String endDateTxt = "";

        Column filterColumn = null;
        Column havingColumn = null;
        List<Column> orderByColumns = new ArrayList<>();
        List<Boolean> isAscs = new ArrayList<>();
        List<Column> caseSearchConditionList = new ArrayList<>();
        List<Column> caseOutputConditionList = new ArrayList<>();
        String timestampFieldFullName;
        int limit = Integer.MAX_VALUE;

        boolean hasDerivedSelection = false;

        public String getMainTable() {
            return mainTable;
        }

        public Column[] getOrderByColumns() {
            return orderByColumns.toArray(new Column[orderByColumns.size()]);
        }

        public Boolean[] getIsAscs() {
            return isAscs.toArray(new Boolean[isAscs.size()]);
        }

        public Map<String, Field> getBaseFieldMap() {
            return baseFieldMap;
        }

        public Map<String, Field> getNestedFuncFieldMap() {
            return nestedFuncFieldMap;
        }

        public List<Column> getNestedFuncColumns() {
            return nestedFuncColumns;
        }

        public Column getHavingColumn() {
            return havingColumn;
        }

        public Column[] getSelectColumns() {
            return selectColumnList.toArray(new Column[selectColumnList.size()]);
        }

        public Column getFilterColumn() {
            return filterColumn;
        }

        public Map<String, RecordBufferMeta> getTableColumnMetadataMap() {
            return tableColumnMetadataMap;
        }

        public String getStartDateTxt() {
            return startDateTxt;
        }

        public long getStartTimeStamp() {
            return timeList.get(0);
        }

        public long getEndTimeStamp() {
            return timeList.get(1);
        }

        public Column[] getBaseColumns() {
            return baseColumns.toArray(new Column[baseColumns.size()]);
        }

        public Column[] getMapColumns() {
            return mapColumnMap.values().toArray(new Column[mapColumnMap.size()]);
        }

        public Column[] getKeyColumns() {
            Column[] cols = new Column[0];
            int count = 0;
            for (Column col : mapColumnMap.values()) {
                if (!col.hasAggregation()) {
                    count ++;
                }
            }
            if (count > 0) {
                cols = new Column[count];
                int i = 0;
                for (Column col : mapColumnMap.values()) {
                    if (!col.hasAggregation()) {
                        cols[i++] = col;
                    }
                }
            }
            return cols;
        }

        public Column[] getAggColumns() {
            Column[] cols = new Column[0];
            int count = 0;
            for (Column col : mapColumnMap.values()) {
                if (col.hasAggregation()) {
                    count ++;
                }
            }
            if (count > 0) {
                cols = new Column[count];
                int i = 0;
                for (Column col : mapColumnMap.values()) {
                    if (col.hasAggregation()) {
                        cols[i++] = col;
                    }
                }
            }
            return cols;
        }

        public Set<String> getWhereTables() {
            return whereTables;
        }

        public String getTimestampFieldFullName() {
            return timestampFieldFullName;
        }

        public int getLimit() {
            return limit;
        }

        public boolean hasDerivedSelection() {
            return hasDerivedSelection;
        }
    }

    ParsingResult parsingResult = null;
    Stack<Column> expressionStack = null;
    Stack<Boolean> hasAggregationInSearchConditionStack = null;
    String nestedFuncTableName = null;
    Stack<DateTimeZone> timeZoneStack = null;
    boolean hasAggregation = false;
    boolean inWhereClause = false;
    boolean inHavingClause = false;
    boolean inCaseClause = false;
    boolean isAscOrdering = true;

    // flags for nesting functions
    boolean isNestedFuncColumn = false;
    //    boolean inNestedFunc = false;
    Stack<Boolean> inNestedFuncFlagStack = null;
    //    boolean canCacheNestedFuncResult = true;
    Stack<Boolean> canCacheNestedFuncResultFlagStack = null;

    @Override
    public void enterSelect_list_elem(PQLParser.Select_list_elemContext ctx) {
        hasAggregation = false;
    }

    @Override
    public void exitSelect_list_elem(PQLParser.Select_list_elemContext ctx) {
        ParseTree aliasCtx = ctx.column_alias();
        String alias = aliasCtx == null ? "" : aliasCtx.getText();
        Column col = expressionStack.pop();
        col.setAlias(alias);
        if (!hasAggregation) {
            if (col.hasOperation() && col.getOperation() instanceof Constant) {
                parsingResult.mapColumnMap.put(col.getName(), col);
            } else {
                parsingResult.mapColumnMap.put(col.getID(), col);
            }
        } else {
            col.setHasAggregation(true);
        }
        parsingResult.selectColumnList.add(col);
    }



    @Override
    public void enterAggregateWindowedFunction(PQLParser.AggregateWindowedFunctionContext ctx) {
        hasAggregation = true;
    }

    @Override
    public void exitTimestampConversion(PQLParser.TimestampConversionContext ctx) {
        Column col = expressionStack.pop();
        GetTimestamp opr = null;
        try {
            if (ctx.TO_TIMESTAMP() != null) {
                opr = new GetTimestamp(col);
            }

            if (!timeZoneStack.isEmpty()) {
                String timeZoneId = timeZoneStack.pop().getID();
                opr.setTimeZoneId(timeZoneId);
                LOG.debug(String.format("Time zone id is %s", timeZoneId));
            }

            if (ctx.pattern != null) {
                String pattern = ctx.pattern.getText();
                if (StringUtils.isNotBlank(pattern)) {
                    LOG.debug(String.format("Date pattern in is %s", pattern));
                    opr.setPattern(pattern);
                }
            }

            Column oprCol = Column.newInstance(
                    opr.getID(), opr.getResultValueType(), opr, null);
            expressionStack.push(oprCol);

        } catch (Exception e) {
            logAndThrowException(e, "Cannot finish parsing on " + ctx.TO_TIMESTAMP() + " function: " + ctx.getText());
        }
    }

    void logAndThrowException(Exception e, String msg) {
        msg  = "PQL Syntax Error: " + msg;
        LOG.error(msg, e);
        throw new IllegalArgumentException(msg, e);
    }

    @Override
    public void exitDate_conversion(PQLParser.Date_conversionContext ctx) {
        // 1. read the expression for date
        Column col = expressionStack.pop();

        // 2. set the expression for timezone
        Constant tz = new Constant(new Value("UTC", ValueUtils.StringDataType));
        Column tzCol = Column.newInstance("StringConst-" + tz.getID(), tz);
        if (ctx.time_zone() != null) {
            tzCol = expressionStack.pop();
        }
        GetTemporalString opr = null;
        try {
            if (ctx.DATEOF() != null) {
                opr = new GetDate(col, tzCol);
            } else if (ctx.MONTHOF() != null) {
                opr = new GetMonth(col, tzCol);
            } else if (ctx.HOUROF() != null) {
                opr = new GetHour(col, tzCol);
            } else if (ctx.DAYINWEEK() != null) {
                opr = new GetDayInWeek(col, tzCol);
            } else if (ctx.WEEKINYEAR() != null) {
                opr = new GetWeekInYear(col, tzCol);
            } else if (ctx.YEAROF() != null) {
                opr = new GetYear(col, tzCol);
            }

            if (ctx.pattern != null) {
                String pattern = ctx.pattern.getText();
                if (StringUtils.isNotBlank(pattern)) {
                    LOG.debug(String.format("Date pattern in is %s", pattern));
                    opr.setPattern(pattern);
                }
            }

            Column oprCol = Column.newInstance(
                    opr.getID(), opr.getResultValueType(), opr, null);
            expressionStack.push(oprCol);

        } catch (Exception e) {
            logAndThrowException(e, "Cannot finish parsing on " + ctx.opr + " function: " + ctx.getText());
        }
    }

    @Override
    public void exitHistCountAggregateWindowFunction(PQLParser.HistCountAggregateWindowFunctionContext ctx) {
        Column distCol = null;
        if (ctx.DISTINCT() != null) {
            distCol = expressionStack.pop();
        }
        Column binCol = expressionStack.pop();
        try {
            AggOperation aggOpr = ctx.DISTINCT() == null ?
                    new HistCount(binCol) : new HistDistinctCount2(binCol, distCol);
            Column countCol = Column.newInstance(aggOpr.getID(), aggOpr);
            countCol.setHasAggregation(true);
            parsingResult.mapColumnMap.put(countCol.getID(), countCol);
            Column selColumn = Column.newInstance(countCol);
            selColumn.setOperation(null);
            expressionStack.push(selColumn);
        } catch (Exception e) {
            logAndThrowException(e, "Cannot finish parsing on hist_count function: " + ctx.toString());
        }
    }


    @Override
    public void exitHistCountAggregateWindowFunction2(PQLParser.HistCountAggregateWindowFunction2Context ctx) {
        Column distCol = null;
        if (ctx.DISTINCT() != null) {
            distCol = expressionStack.pop();
        }
        Column binCol = expressionStack.pop();
        try {
            int threshold = 1;
            if (ctx.DECIMAL() != null) {
                threshold = Integer.parseInt(ctx.DECIMAL().getText());
            }
            AggOperation aggOpr = ctx.DISTINCT() == null ?
                    new HistCount(binCol) : new HistDistinctCount(binCol, distCol, threshold);
            Column countCol = Column.newInstance(aggOpr.getID(), aggOpr);
            countCol.setHasAggregation(true);
            parsingResult.mapColumnMap.put(countCol.getID(), countCol);
            Column selColumn = Column.newInstance(countCol);
            selColumn.setOperation(null);
            expressionStack.push(selColumn);
        } catch (Exception e) {
            logAndThrowException(e, "Cannot finish parsing on hist_count function: " + ctx.toString());
        }
    }

    @Override
    public void exitCountAggregateWindowFunction(PQLParser.CountAggregateWindowFunctionContext ctx) {
        ParseTree distinctExpression = ctx.distinct_expression();
        try {
            AggOperation aggOpr = distinctExpression == null ?
                    new Count() : new CountDistinct(expressionStack.pop());
            Column countCol = Column.newInstance(aggOpr.getID(), aggOpr);
            countCol.setHasAggregation(true);
            parsingResult.mapColumnMap.put(countCol.getID(), countCol);
            Column selColumn = Column.newInstance(countCol);
            selColumn.setOperation(null);
            expressionStack.push(selColumn);
        } catch (Exception e) {
            logAndThrowException(e, "Cannot finish parsing on count function: " + ctx.toString());
        }
    }

    @Override
    public void exitStandardAggregateWindowFunction(PQLParser.StandardAggregateWindowFunctionContext ctx) {
        Column col = expressionStack.pop();
        AggOperation aggOpr = null;
        Column aggCol = null;
        try {
            if (ctx.SUM() != null) {
                aggOpr = ctx.all_distinct_expression().DISTINCT() != null ?
                        new SumDistinct(col) : new Sum(col);
            } else if (ctx.MAX() != null) {
                aggOpr = new Max(col);
            } else if (ctx.MIN() != null) {
                aggOpr = new Min(col);
            }
        } catch (Exception e) {
            logAndThrowException(e, "Cannot finish parsing on aggregate function: " + ctx.toString());
        }

        aggCol = Column.newInstance(aggOpr.getID(), aggOpr);
        aggCol.setHasAggregation(true);
        parsingResult.mapColumnMap.put(aggCol.getID(), aggCol);
        Column selCol = Column.newInstance(aggCol);
        selCol.setOperation(null);
        expressionStack.push(selCol);
    }

    @Override
    public void exitConstant(PQLParser.ConstantContext ctx) {
        int sign = 1;

        if (ctx.sign() != null && ctx.sign().getText().equals("-")) {
            sign = -1;
        }
        try {
            if (ctx.STRING() != null) {
                String content = ctx.STRING().getText();
                Constant cont = new Constant(
                        new Value(content.substring(1, content.length()-1), ValueUtils.StringDataType));
                expressionStack.push(
                        Column.newInstance("StringConst-" + cont.getID(), cont));
            } else if (ctx.DECIMAL() != null) {
                String numberTxt = ctx.DECIMAL().getText();
                long number = Long.parseLong(numberTxt) * sign;
                Constant cont = new Constant(new Value(number, ValueUtils.LongDataType));
                expressionStack.push(
                        Column.newInstance("DecimalConst-" + numberTxt, cont));
            } else if (ctx.FLOAT() != null) {
                String floatTxt = ctx.FLOAT().getText();
                double val = Double.parseDouble(floatTxt) * sign;
                Constant cont = new Constant(new Value(val, ValueUtils.FloatDataType));
                expressionStack.push(
                        Column.newInstance("FloatConst-" + floatTxt, cont)
                );
            }
        } catch (Exception e) {
            logAndThrowException(e, "Cannot get a constant value from " + ctx.toString());
        }

    }

    @Override
    public void enterNf_full_column_name(PQLParser.Nf_full_column_nameContext ctx) {
        isNestedFuncColumn = true;
    }

    @Override
    public void exitNf_full_column_name(PQLParser.Nf_full_column_nameContext ctx) {
        isNestedFuncColumn = false;
    }

    @Override
    public void exitFull_column_name(PQLParser.Full_column_nameContext ctx) {
        // valid the column name and table name
        String tableName = parsingResult.mainTable;
        boolean hasOriginalFullName = true;
        if (ctx.table_name() != null && !ctx.table_name().getText().isEmpty()) {
            tableName = ctx.table_name().getText();
            // recover the full column name
            if (!tableName.startsWith(parsingResult.mainTable)) {
                tableName = parsingResult.mainTable + "." + tableName;
                hasOriginalFullName = false;
            }
        }

        String colName = ctx.ID().getText();
        String fullColName = tableName + "." + colName;
        expressionStack.push(addBasicField(fullColName, hasOriginalFullName));
        LOG.debug("YQ:"+ctx.getText());
    }

    private boolean inNestedFunc() {
        return inNestedFuncFlagStack!=null &&
                !inNestedFuncFlagStack.isEmpty() &&
                inNestedFuncFlagStack.peek();
    }

    private boolean canCacheNestedFuncResult() {
        return canCacheNestedFuncResultFlagStack!=null &&
                !canCacheNestedFuncResultFlagStack.isEmpty() &&
                canCacheNestedFuncResultFlagStack.peek();
    }

    private Column addBasicField(String fieldFullName, boolean hasOriginalFullName) {
        Column col;
        Field field;
        FieldType type;
        String colName;
        String tableName;
        int lastDotIndex = fieldFullName.lastIndexOf('.');
        if (lastDotIndex > 0) {
            colName = fieldFullName.substring(lastDotIndex + 1);
            tableName = fieldFullName.substring(0, lastDotIndex);
        } else {
            String msg = String.format(
                    "The name of field (%s) should include table name!", fieldFullName);
            LOG.error(msg);
            throw new IllegalArgumentException(msg);
        }

        field = getFeild(tableName, colName);
        type = getType(tableName, colName);

        if (field.isDerived()) {
            parsingResult.hasDerivedSelection = true;
        }

        boolean isNFColumn = isNestedFuncColumn;
        if (hasOriginalFullName && inNestedFunc() && !isNestedFuncColumn) {
            isNFColumn = tableName.startsWith(nestedFuncTableName);
        }

        if (isNFColumn) {
            String nfFieldFullName = "nf." + fieldFullName;
            // validate it must be
            int typeId = type.getID();

            if (typeId == PrimitiveType.INT64.getId()
                    || typeId == PrimitiveType.INT32.getId()
                    || typeId == PrimitiveType.INT16.getId()) {

                col = Column.newInstance(nfFieldFullName, ValueUtils.ListLongDataType);

            } else if (typeId == ValueUtils.FloatDataType
                    || typeId == ValueUtils.DoubleDataType) {

                col = Column.newInstance(nfFieldFullName, ValueUtils.ListFloatDataType);

            } else if (typeId == ValueUtils.StringDataType) {

                col = Column.newInstance(nfFieldFullName, ValueUtils.ListStringDataType);

            } else {
                String msg = String.format(
                        "Not support nested function on %s of type %d", fieldFullName, typeId);
                LOG.error(msg);
                throw new IllegalArgumentException(msg);
            }

            parsingResult.nestedFuncFieldMap.put(nfFieldFullName, field);
            if (isNestedFuncColumn) {
                // isNestedFuncColumn can be true only once per nested function
                nestedFuncTableName = tableName;
            }
        } else {
            col = Column.newInstance(fieldFullName, type.getID());
            parsingResult.baseColumns.add(col);
            parsingResult.baseFieldMap.put(fieldFullName, field);
            if (inNestedFunc()) {
                canCacheNestedFuncResultFlagStack.pop();
                canCacheNestedFuncResultFlagStack.push(false);
            }
        }

        LOG.info(String.format("A new column: %s", col.getID()));
        return col;
    }

    private Field getFeild(String tableName, String fieldName) {
        RecordBufferMeta tableMeta = parsingResult.tableColumnMetadataMap.get(tableName);
        if (tableMeta != null) {
            RecordMetadataInterface fieldMeta = (RecordMetadataInterface) tableMeta.getFieldMeta(fieldName);
            if (fieldMeta == null) {
                String msg = fieldName + " does not exist in the table : " + tableMeta;
                LOG.error(msg);
                throw new IllegalArgumentException(msg);
            }
            return fieldMeta.getField();
        }
        return null;
    }

    private FieldType getType(String tableName, String fieldName) {
        Field field = getFeild(tableName, fieldName);
        if (field != null) {
            return field.getType();
        } else {
            return FieldTypeFactory.getPrimitiveType(PrimitiveType.UNKNOWN);
        }
    }

    @Override
    public void enterNestingWindowedFunction(PQLParser.NestingWindowedFunctionContext ctx) {
        canCacheNestedFuncResultFlagStack.push(true);
        inNestedFuncFlagStack.push(true);
    }

    @Override
    public void exitNestingWindowedFunction(PQLParser.NestingWindowedFunctionContext ctx) {
        // pop the table name from stack
        nestedFuncTableName = "";
        inNestedFuncFlagStack.pop();
        canCacheNestedFuncResultFlagStack.pop();
    }

    @Override
    public void exitNfCountFunction(PQLParser.NfCountFunctionContext ctx) {
        Column predicate = null;
        if (ctx.search_condition() != null) {
            predicate = expressionStack.pop();
        }
        Column target = expressionStack.pop();
        long limit = Long.MAX_VALUE;
        if (ctx.LIMIT() != null) {
            limit = Long.parseLong(ctx.DECIMAL().getText());
        }
        Operation opr;
        if (ctx.DISTINCT() != null) {
            opr = new NestedListCountDistinct(target, predicate, canCacheNestedFuncResult(), limit);
        } else {
            opr = new NestedListCount(target, predicate, canCacheNestedFuncResult(), limit);
        }
        Column col = Column.newInstance(opr.getID(), opr);
        parsingResult.nestedFuncColumns.add(col);
        expressionStack.push(col);
    }

    @Override
    public void exitNfStandardFuncion(PQLParser.NfStandardFuncionContext ctx) {
        Column predicate = null;
        if (ctx.search_condition() != null) {
            predicate = expressionStack.pop();
        }
        Column target = expressionStack.pop();
        long limit = Long.MAX_VALUE;
        if (ctx.LIMIT() != null) {
            limit = Long.parseLong(ctx.DECIMAL().getText());
        }
        Operation opr;
        if (ctx.NF_MAX() != null) {
            opr = new NestedListMax(target, predicate, canCacheNestedFuncResult(), limit);
        } else if (ctx.NF_MIN() != null) {
            opr = new NestedListMin(target, predicate, canCacheNestedFuncResult(), limit);
        } else if (ctx.NF_SUM() != null) {
            opr = new NestedListSum(target, predicate, canCacheNestedFuncResult(), limit);
        } else {
            String msg = "Cannot create standard function from : " + ctx;
            LOG.error(msg);
            throw new IllegalArgumentException(msg);
        }
        Column col = Column.newInstance(opr.getID(), opr);
        parsingResult.nestedFuncColumns.add(col);
        expressionStack.push(col);
    }

    @Override
    public void exitPlaySessionCountFunction(PQLParser.PlaySessionCountFunctionContext ctx) {
        Column predicate = null;
        if (ctx.search_condition() != null) {
            predicate = expressionStack.pop();
        }
        Column duration = expressionStack.pop();
        Column start = expressionStack.pop();
        Column target = expressionStack.pop();
        long limit = Long.MAX_VALUE;
        long timeout = Long.MAX_VALUE;
        long minDuration = 0;

        int minDurationIndex = -1;
        if (ctx.MIN_DURATION_IN_MS() != null) {
            minDurationIndex ++;
            minDuration = Long.parseLong(ctx.DECIMAL(minDurationIndex).getText());
        }

        int timeOutIndex = minDurationIndex;
        if (ctx.TIMEOUT() != null) {
            timeOutIndex ++;
            timeout = Long.parseLong(ctx.DECIMAL(timeOutIndex).getText());
        }

        if (ctx.LIMIT() != null) {
            int limitIndex = timeOutIndex + 1;
            limit = Long.parseLong(ctx.DECIMAL(limitIndex).getText());
        }

        Operation opr = new PlaySessionCount(target, start, duration,
                predicate, canCacheNestedFuncResult(), minDuration, limit, timeout);
        Column col = Column.newInstance(opr.getID(), opr);
        parsingResult.nestedFuncColumns.add(col);
        expressionStack.push(col);
    }

//    @Override
//    public void exitPlaySessionCountFunctionTmp(PQLParser.PlaySessionCountFunctionTmpContext ctx) {
//        Column predicate = null;
//        if (ctx.search_condition() != null) {
//            predicate = expressionStack.pop();
//        }
//        Column duration = expressionStack.pop();
//        Column start = expressionStack.pop();
//        Column target = expressionStack.pop();
//        long limit = Long.MAX_VALUE;
//        long timeout = Long.MAX_VALUE;
//        long minDuration = 0;
//
//        int minDurationIndex = -1;
//        if (ctx.MIN_DURATION_IN_MS() != null) {
//            minDurationIndex ++;
//            minDuration = Long.parseLong(ctx.DECIMAL(minDurationIndex).getText());
//        }
//
//        int timeOutIndex = minDurationIndex;
//        if (ctx.TIMEOUT() != null) {
//            timeOutIndex ++;
//            timeout = Long.parseLong(ctx.DECIMAL(timeOutIndex).getText());
//        }
//
//        if (ctx.LIMIT() != null) {
//            int limitIndex = timeOutIndex + 1;
//            limit = Long.parseLong(ctx.DECIMAL(limitIndex).getText());
//        }
//
//        Operation opr = new PlaySessionCount2(target, start, duration,
//            predicate, canCacheNestedFuncResult(), minDuration, limit, timeout);
//        Column col = Column.newInstance(opr.getID(), opr);
//        parsingResult.nestedFuncColumns.add(col);
//        expressionStack.push(col);
//    }

    @Override
    public void exitComparison_expression(PQLParser.Comparison_expressionContext ctx) {
        ParseTree comparisonOprCtx = ctx.comparison_operator();
        String parsedTxt = ctx.getText();
        LOG.debug("Parsing the comparison expression - " + parsedTxt);
        Column right = expressionStack.pop();
        Column left = expressionStack.pop();
        Operation operation;
        try {
            operation = getComparisonOpr(left, right, comparisonOprCtx.getText());
            expressionStack.push(Column.newInstance(operation.getID(), operation));
        } catch (Exception ex) {
            logAndThrowException(ex, "Cannot parse the comparison expression in Predication: " + ctx.getText());
        }
    }

    @Override
    public void exitBinary_boolean_operator_expression(PQLParser.Binary_boolean_operator_expressionContext ctx) {
        ParseTree comparisonOprCtx = ctx.comparison_operator();
        String parsedTxt = ctx.getText();
        LOG.debug("Parsing the comparison expression - " + parsedTxt);
        Column right = expressionStack.pop();
        Column left = expressionStack.pop();
        Operation operation = null;
        try {
            operation = getComparisonOpr(left, right, comparisonOprCtx.getText());
        } catch (Exception ex) {
            logAndThrowException(ex, "Cannot parse the comparison expression in Expression: " + ctx.getText());
        }
        expressionStack.push(Column.newInstance(operation.getID(), operation));
    }

    private Operation getComparisonOpr(Column left, Column right, String oprStr) throws Exception {
        Operation operation = null;
        switch (oprStr) {
            case "=":
                operation = new Equal(left, right);
                break;
            case "<>":
            case "!=":
                operation = new NotEqual(left, right);
                break;
            case "<":
                operation = new Less(left, right);
                break;
            case "<=":
            case "!>":
                operation = new LessOrEqual(left, right);
                break;
            case ">":
                operation = new Greater(left, right);
                break;
            case ">=":
            case "!<":
                operation = new GreaterOrEqual(left, right);
                break;
        }
        return operation;
    }

    @Override
    public void exitCastLongFunction(PQLParser.CastLongFunctionContext ctx) {
        Column col = expressionStack.pop();
        try {
            CastLong cl = new CastLong(col);
            expressionStack.push(Column.newInstance(cl.getID(), cl));
        } catch (Exception e) {
            logAndThrowException(e, "Cannot parse the CastLong function: " + ctx.getText());
        }
    }

    @Override
    public void exitGetFromJsonFunction(PQLParser.GetFromJsonFunctionContext ctx) {
        Column queryCol = expressionStack.pop();
        Column jsonCol = expressionStack.pop();
        try {
            GetJsonValue gj = new GetJsonValue(jsonCol, queryCol);
            expressionStack.push(Column.newInstance(gj.getID(), gj));
        } catch (Exception e) {
            logAndThrowException(e, "Cannot parse the GetFronJson function: " + ctx.getText());
        }
    }

    @Override
    public void exitSubStringFunction(PQLParser.SubStringFunctionContext ctx) {
        Column endPosCol = ctx.end == null ? null : expressionStack.pop();
        Column startPosCol = expressionStack.pop();
        Column strCol = expressionStack.pop();
        try {
            Substr ss = endPosCol == null ? new Substr(strCol, startPosCol) : new Substr(strCol, startPosCol, endPosCol);
            expressionStack.push(Column.newInstance(ss.getID(), ss));
        } catch (Exception e) {
            logAndThrowException(e, "Cannot parse the Substr function: " + ctx.getText());
        }
    }

    @Override
    public void exitExtractStringFunction(PQLParser.ExtractStringFunctionContext ctx) {
        Column txtCol = expressionStack.pop();
        String regx = ctx.STRING().getText();
        regx = regx.substring(1, regx.length() - 1);// remove the ' at the ends
        int index = 0;
        try {
            if (ctx.DECIMAL() != null) {
                index = Integer.parseInt(ctx.DECIMAL().getText());
            }
        } catch (NumberFormatException e) {
            logAndThrowException(e, "The function of extracting a match from a string needs a non-negative index");
        }

        try {
            ExtStr es = new ExtStr(txtCol, regx, index);
            expressionStack.push(Column.newInstance(es.getID(), es));
        } catch (Exception e) {
            logAndThrowException(e, "Cannot parse the ExtStr function - " + ctx.getText());
        }
    }

    @Override
    public void exitConcatFunction(PQLParser.ConcatFunctionContext ctx) {
        int expNum = ctx.expression_list().expression().size();
        List<Column> cols = new ArrayList<>();
        try {
            for (int i = 0; i < expNum; ++i) {
                cols.add(0, expressionStack.pop());
            }
            Concat coal = new Concat(cols);
            expressionStack.push(Column.newInstance(coal.getID(), coal));
        } catch (Exception e) {
            logAndThrowException(e, "Cannot parse the concat function: " + ctx.getText());
        }
    }

    @Override
    public void exitCoalesceFunction(PQLParser.CoalesceFunctionContext ctx) {
        int expNum = ctx.expression_list().expression().size();
        List<Column> cols = new ArrayList<>();
        try {
            for (int i = 0; i < expNum; ++i) {
                cols.add(0, expressionStack.pop());
            }
            Coalesce coal = new Coalesce(cols);
            expressionStack.push(Column.newInstance(coal.getID(), coal));
        } catch (Exception e) {
            logAndThrowException(e, "Cannot parse the coalesce function: " + ctx.getText());
        }
    }

    @Override
    public void exitHashFunction(PQLParser.HashFunctionContext ctx) {
        Column col = expressionStack.pop();
        try {
            Hash cl = new Hash(col);
            expressionStack.push(Column.newInstance(cl.getID(), cl));
        } catch (Exception e) {
            logAndThrowException(e, "Cannot parse the hash function: " + ctx.getText());
        }
    }

    @Override
    public void exitLike_expression(PQLParser.Like_expressionContext ctx) {
        boolean hasNOT = ctx.NOT() != null;
        Column right = expressionStack.pop();
        Column left = expressionStack.pop();
        Column col;
        try {
            Like like = new Like(left, right);
            col = Column.newInstance(like.getID(), like);
            if (hasNOT) {
                Not not = new Not(col);
                col = Column.newInstance(not.getID(), not);
            }
            expressionStack.push(col);
        } catch (Exception ex) {
            logAndThrowException(ex, "Cannot parse the like expression: " + ctx.getText());
        }
    }

    @Override
    public void exitIn_expression(PQLParser.In_expressionContext ctx) {
        boolean hasNOT = ctx.NOT() != null;
        int numOfExpr = ctx.expression_list().expression().size();
        Column inCol;
        try {

            //1. fetch all expressions in the list
            List<Value> selectList = new ArrayList<>();
            for (int i = 0; i < numOfExpr; ++i) {
                Column col = expressionStack.pop();
                Constant cont = (Constant) col.getOperation();
                selectList.add(cont.apply(null));
            }

            //2.
            Column leftCol = expressionStack.pop();
            In in = new In(leftCol);
            in.setValueList(selectList);
            inCol = Column.newInstance(in.getID(), in);
            if (hasNOT) {
                Not not = new Not(inCol);
                inCol = Column.newInstance(not.getID(), not);
            }
            expressionStack.push(inCol);
        } catch (Exception ex) {
            logAndThrowException(ex, "Cannot parse the IN expression: " + ctx.getText());
        }
    }

    @Override
    public void exitBinFunction(PQLParser.BinFunctionContext ctx) {
        Column minCol = null;
        Column maxCol = null;
        Column startCol = null;
        Column durationCol = null;
        Column sizeCol = null;
        if (ctx.min != null) {
            minCol = expressionStack.pop();
            maxCol = expressionStack.pop();
        }
        sizeCol = expressionStack.pop();
        durationCol = expressionStack.pop();
        startCol = expressionStack.pop();
        try {
            Operation opr = null;
            if (minCol != null) {
                opr = new Bin(startCol, durationCol, sizeCol, maxCol, minCol);
            } else {
                opr = new Bin(startCol, durationCol, sizeCol);
            }
            expressionStack.push(Column.newInstance(opr.getID(), opr));
        } catch (Exception ex) {
            logAndThrowException(ex, "Cannot parse the bin expression: " + ctx.getText());
        }
    }

    @Override
    public void exitIs_null_or_not_null(PQLParser.Is_null_or_not_nullContext ctx) {
        Column col = expressionStack.pop();
        boolean isNotNull = false;
        if (ctx.null_notnull().NOT() != null) {
            isNotNull = true;
        }
        try {
            IsNull isNull = new IsNull(col, isNotNull);
            expressionStack.push(Column.newInstance(isNull.getID(), isNull));
        } catch (Exception ex) {
            logAndThrowException(ex, "Cannot parse the isNull or isNotNull expression: " + ctx.getText());
        }
    }

    @Override
    public void exitNot_operator_expression(PQLParser.Not_operator_expressionContext ctx) {
        Column col = expressionStack.pop();
        if (col.getType() != ValueUtils.BooleanDataType) {
            String msg = "Cannot apply NOT to non-boolean value: " + col.getID();
            LOG.error(msg);
            throw new IllegalArgumentException(msg);
        }

        try {
            Not not = new Not(col);
            expressionStack.push(Column.newInstance(not.getID(), not));
        } catch (Exception e) {
            logAndThrowException(e, "Cannot parse the ~expression: " + ctx.getText());
        }
    }

    @Override
    public void exitPosneg_operator_expression(PQLParser.Posneg_operator_expressionContext ctx) {
        Column col = expressionStack.pop();
        try {
            switch (ctx.op.getText()) {
                case "+":
                    Positive pos = new Positive(col);
                    expressionStack.push(Column.newInstance(pos.getID(), pos));
                    break;
                case "-":
                    Negative neg = new Negative(col);
                    expressionStack.push(Column.newInstance(neg.getID(), neg));
                    break;
            }
        } catch (Exception ex) {
            logAndThrowException(ex, "Cannot parse the (+|-) operation: " + ctx.getText());
        }
    }

    @Override
    public void exitBinary_operator_expression(PQLParser.Binary_operator_expressionContext ctx) {
        Column rCol = expressionStack.pop();
        Column lCol = expressionStack.pop();
        try {
            Operation operation = null;
            switch (ctx.op.getText()) {
                case "+":
                    operation = new Addition(lCol, rCol);
                    break;
                case "-":
                    operation = new Subtraction(lCol, rCol);
                    break;
                case "*":
                    operation = new Multiplication(lCol, rCol);
                    break;
                case "/":
                    operation = new Division(lCol, rCol);
                    break;
                case "%":
                    operation = new Modulo(lCol, rCol);
                    break;
                case "|":
                case "&":
                case "^":
                    String msg = "Don't support the binary operation for now - " + ctx.op.getText();
                    LOG.error(msg);
                    throw new IllegalArgumentException(msg);
            }
            expressionStack.push(Column.newInstance(operation.getID(), operation));
        } catch (Exception ex) {
            logAndThrowException(ex, "Cannot parse the (+|-|*|/|%) operation: " + ctx.getText());
        }
    }

    @Override
    public void exitOrder_by_expression(PQLParser.Order_by_expressionContext ctx) {
        Column col = expressionStack.pop();
        if (ctx.DESC() != null) {
            isAscOrdering = false;
        }
        // check if col is in the select list
        boolean hasFound = false;
        for (Column cur : parsingResult.selectColumnList) {
            if (col.getID().equals(cur.getID())) {
                parsingResult.orderByColumns.add(col);
                parsingResult.isAscs.add(isAscOrdering);
                hasFound = true;
                break;
            }
        }
        isAscOrdering = true;
        if (!hasFound) {
            String msg = col.getID() + " should be in the select list";
            LOG.error(msg);
            throw new IllegalArgumentException(msg);
        }
    }

    @Override
    public void exitSearch_condition_not(PQLParser.Search_condition_notContext ctx) {
        try {
            if (ctx.NOT() != null) {
                // there is a Not
                Column col = expressionStack.pop();
                Not not = new Not(col);
                expressionStack.push(Column.newInstance(not.getID(), not));
            }
        } catch (Exception e) {
            logAndThrowException(e, "Cannot parse the search condition - " + ctx);
        }
    }

    @Override
    public void exitSearch_condition_and(PQLParser.Search_condition_andContext ctx) {
        int numOfItems = ctx.search_condition_not().size();
        Column[] cols = new Column[numOfItems];
        for (int i=0; i<numOfItems; ++i) {
            cols[i] = expressionStack.pop();
        }
        try {
            Column base = cols[0];
            for (int i = 1; i < numOfItems; ++i) {
                And and = new And(base, cols[i]);
                base = Column.newInstance(and.getID(), and);
            }
            expressionStack.push(base);
        } catch (Exception ex) {
            logAndThrowException(ex, "Cannot parse the AND operation: " + ctx.getText());
        }
    }

    @Override
    public void exitNested_search_condition(PQLParser.Nested_search_conditionContext ctx) {
        int numOfItems = ctx.nesting_search_condition().search_condition_and().size();
        Column[] cols = new Column[numOfItems];
        for (int i=0; i<numOfItems; ++i) {
            cols[i] = expressionStack.pop();
        }

        Column base = cols[0];
        try {
            for (int i=1; i<numOfItems; ++i) {
                OR or = new OR(base, cols[i]);
                base = Column.newInstance(or.getID(), or);
            }
            expressionStack.push(base);
        } catch (Exception ex) {
            logAndThrowException(ex, "Cannot parse the OR operation: " + ctx.getText());
        }
    }

    @Override
    public void enterSearch_condition(PQLParser.Search_conditionContext ctx) {
        ParseTree parentCtx = ctx.getParent();
        if (parentCtx instanceof PQLParser.Select_statementContext) {
            if (ctx == ((PQLParser.Select_statementContext) parentCtx).where) {
                inWhereClause = true;
            } else if (ctx == ((PQLParser.Select_statementContext) parentCtx).having) {
                inHavingClause = true;
            }
        } else if (parentCtx instanceof PQLParser.Switch_search_condition_sectionContext) {
            inCaseClause = true;
        }
        hasAggregationInSearchConditionStack.push(hasAggregation);
        hasAggregation = false;
    }


    @Override
    public void exitSearch_condition(PQLParser.Search_conditionContext ctx) {
        int numOfItems = ctx.search_condition_and().size();
        Column[] cols = new Column[numOfItems];
        for (int i=0; i<numOfItems; ++i) {
            cols[i] = expressionStack.pop();
        }

        Column base = cols[0];
        try {
            for (int i=1; i<numOfItems; ++i) {
                OR or = new OR(base, cols[i]);
                base = Column.newInstance(or.getID(), or);
            }
        } catch (Exception ex) {
            logAndThrowException(ex, "Cannot parse the OR operation: " + ctx.getText());
        }

        if (inWhereClause) {
            parsingResult.filterColumn = base;
            if (hasAggregation) {
                String msg = "Aggregation cannot be in the WHERE clause - " + ctx;
                LOG.error(msg);
                throw new IllegalArgumentException(msg);
            }
        }

        if (inHavingClause) {
            parsingResult.havingColumn = base;
            if (!hasAggregation) {
                String msg = "Having clause is used for aggregates - " + ctx;
                LOG.error(msg);
                throw new IllegalArgumentException(msg);
            }

            if (!base.applicable(parsingResult.mapColumnMap)) {
                String msg = "Having clause should only contain columns in the selection list - " + ctx;
                LOG.error(msg);
                throw new IllegalArgumentException(msg);
            }
        }

        if (inCaseClause || inNestedFunc()) {
            expressionStack.push(base);
        }

        // update flags
        ParseTree parentCtx = ctx.getParent();
        if (parentCtx instanceof PQLParser.Select_statementContext) {
            if (ctx == ((PQLParser.Select_statementContext) parentCtx).where) {
                inWhereClause = false;
            } else if (ctx == ((PQLParser.Select_statementContext) parentCtx).having) {
                inHavingClause = false;
            }
        } else if (parentCtx instanceof PQLParser.Switch_search_condition_sectionContext) {
            inCaseClause = false;
        }

        hasAggregation = hasAggregationInSearchConditionStack.pop();
    }

    @Override
    public void enterCase_expression(PQLParser.Case_expressionContext ctx) {
        this.parsingResult.caseOutputConditionList.clear();
        this.parsingResult.caseSearchConditionList.clear();
    }

    @Override
    public void exitCase_expression(PQLParser.Case_expressionContext ctx) {
        Column defaultOutputCol = expressionStack.pop();
        Column firstOutputCol = this.parsingResult.caseOutputConditionList.get(0);

        if (firstOutputCol.getType() != defaultOutputCol.getType()) {
            String msg = "The default case output should be the same " +
                    "type as the previous ones - " + ctx;
            LOG.error(msg);
            throw new IllegalArgumentException(msg);
        }
        this.parsingResult.caseOutputConditionList.add(defaultOutputCol);
        Column[] conditions = this.parsingResult.caseSearchConditionList.toArray(
                new Column[this.parsingResult.caseSearchConditionList.size()]);
        Column[] outputs = this.parsingResult.caseOutputConditionList.toArray(
                new Column[this.parsingResult.caseOutputConditionList.size()]
        );

        try {
            CaseStatement caseStatement = new CaseStatement(conditions, outputs);
            expressionStack.push(Column.newInstance(caseStatement.getID(), caseStatement));
        } catch (Exception ex) {
            logAndThrowException(ex, "Cannot parse the case-switch operation: " + ctx.getText());
        }
    }

    @Override
    public void exitSwitch_search_condition_section(PQLParser.Switch_search_condition_sectionContext ctx) {
        Column outputCol = expressionStack.pop();
        Column conditionCol = expressionStack.pop();
        this.parsingResult.caseOutputConditionList.add(outputCol);
        this.parsingResult.caseSearchConditionList.add(conditionCol);
    }

    @Override
    public void exitTime_window(PQLParser.Time_windowContext ctx) {
        DateTimeZone timeZone = DateTimeZone.UTC;
        if (ctx.time_zone() != null) {
            timeZone = timeZoneStack.pop();
        }

        if (ctx.PAST() != null) {
            int days = Integer.parseInt(ctx.DECIMAL(0).getText());
            if (days > 0) {
                Date end = ParserUtil.getBeginingOfDayForDate(
                        ParserUtil.getCurrentDate(timeZone),
                        timeZone);
                Date start = ParserUtil.getBeginingOfDayForDate(
                        ParserUtil.getDateBeforeNumDays(end, days, timeZone),
                        timeZone);
                addTS(start.getTime(), end.getTime() - 1);
            } else {
                String msg = "Support past positive days only! - " + ctx.DECIMAL();
                LOG.error(msg);
                throw new IllegalArgumentException(msg);
            }
        } else if (ctx.date_expression() != null && ctx.date_expression().size() == 2) {
            parsingResult.startDateTxt = ctx.date_expression(0).DATE().getText();
            parsingResult.endDateTxt = ctx.date_expression(1).DATE().getText();

            int startDiff = 0;
            if (ctx.date_expression(0).DECIMAL() != null) {
                startDiff = Integer.parseInt(ctx.date_expression(0).DECIMAL().getText());
                String sign = ctx.date_expression(0).sign().getText();
                if (ctx.date_expression(0).sign().getText().equals("-")) {
                    startDiff *= -1;
                }
            }

            int endDiff = 0;
            if (ctx.date_expression(1).DECIMAL() != null) {
                endDiff = Integer.parseInt(ctx.date_expression(1).DECIMAL().getText());
                if (ctx.date_expression(1).sign().getText().equals("-")) {
                    endDiff *= -1;
                }
            }
            updateTimeRangeByTimeZone(timeZone, startDiff, endDiff);
        } else {
            LOG.info(String.format("Timestamp Range is %s", ctx.getText()));
            long startTs = Long.parseLong(ctx.DECIMAL(0).getText());
            long endTs = Long.parseLong(ctx.DECIMAL(1).getText());
            addTS(startTs, endTs);
        }
    }

    @Override
    public void exitTime_zone_hr(PQLParser.Time_zone_hrContext ctx) {
        int offsetHour = Integer.parseInt(ctx.getText());
        DateTimeZone timezone = DateTimeZone.forOffsetHours(offsetHour);
        timeZoneStack.push(timezone);
//        updateTimeRangeByTimeZone(timezone);

    }

    @Override
    public void exitTime_zone_id(PQLParser.Time_zone_idContext ctx) {
        DateTimeZone timeZone = DateTimeZone.forID(ctx.getText());
        timeZoneStack.push(timeZone);
//        updateTimeRangeByTimeZone(timeZone);
    }


    /////////////////////////////////////////////////////////////////////
    // Regular Methods
    /////////////////////////////////////////////////////////////////////

    public ProfileQuery(final String metaPath) {
        this.metadataPath = metaPath;
        this.metadataPathList = null;
        init();
    }

    public ProfileQuery(final List<String> metaPath) {
        this.metadataPath = null;
        this.metadataPathList = metaPath;
        init();
    }

    private void init() {
        Collection<Class<? extends RecordMetadataInterface>> tableMetadataSet =
                metadataPath != null ?
                        new GetAllMetadataEnumClasses().apply(metadataPath) :
                        new GetAllMetadataEnumClasses2().apply(metadataPathList);

        if (!tableMetadataSet.isEmpty()) {
            for (@SuppressWarnings("rawtypes") Class cur : tableMetadataSet) {
                RecordBufferMeta tableMeta =  RecordBufferMeta.getRecordOperator(cur);
                tableMetadataMap.put(tableMeta.getTableName(), tableMeta);
            }
        }
        LOG.debug(tableMetadataMap.toString());
    }

    public void parse(final String query) {
        CharStream charStream = CharStreams.fromString(query);
        PQLLexer lexer = new PQLLexer(charStream);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        PQLParser parser = new PQLParser(tokens);
        ParseTreeWalker walker = new ParseTreeWalker();

        // get all tables involved
        ProfileQueryTableListener tableListener = new ProfileQueryTableListener(tableMetadataMap);
        walker.walk(tableListener, parser.pql_clause());

        parsingResult = new ParsingResult(query);
        parsingResult.tableColumnMetadataMap = tableListener.tableColMetaMap;
        parsingResult.whereTables = tableListener.wheretableSet;
        parsingResult.mainTable = tableListener.mainTable;

        // the field of time stamp is a must-have field
        for (String table : tableListener.wheretableSet) {
            if (!table.equals(tableListener.mainTable)) {
                parsingResult.timestampFieldFullName = table + "." + TIMESTAMP_FIELD_SIMPLE_NAME;
                addBasicField(parsingResult.timestampFieldFullName, false);
                break;//fixme not necessary if only no two different subtable occur at the same time
            }
        }

        // initiate members
        hasAggregationInSearchConditionStack = new Stack<>();
        expressionStack = new Stack<>();
        timeZoneStack = new Stack<>();
        canCacheNestedFuncResultFlagStack = new Stack<>();
        inNestedFuncFlagStack = new Stack<>();

        // Traverse the parse tree
        parser.reset();
        walker.walk(this, parser.pql_clause());
    }


    private void addTS(long startTs, long endTs) {
        if (startTs > endTs) {
            throw new IllegalArgumentException("The beginning time stamp should not be larger than the ending time stamp. ");
        }
        parsingResult.timeList.clear();
        parsingResult.timeList.add(startTs);
        parsingResult.timeList.add(endTs);
    }



    private void updateTimeRangeByTimeZone(DateTimeZone dateTimeZone, int startDiff, int endDiff) {
        SimpleDateFormat format = ParserUtil.getDateTimeFormatWithPattern("yyyy/MM/dd");
        try {
            Date start = ParserUtil.getBeginingOfDayForDate(format.parse(parsingResult.startDateTxt), dateTimeZone);
            Date end = ParserUtil.getEndOfDayForDate(format.parse(parsingResult.endDateTxt), dateTimeZone);

            addTS(start.getTime() + startDiff * 24l * 3600000, end.getTime() + endDiff * 24l * 3600000);

        } catch (ParseException e) {
            logAndThrowException(e, "The date should be in the format of yyyy/MM/dd, e.g., 2011/01/02");
        } catch (NullPointerException e) {
            logAndThrowException(e, "The date should be specified, e.g., 2011/01/02 given a time zone");
        }
    }

    public ParsingResult getParsingResult() {
        return parsingResult;
    }

    public Map<String, RecordBufferMeta> getTableMetadataMap() {
        return tableMetadataMap;
    }

    /**
     * It walks through the tree to get all tables involved in the query.
     */
    static class ProfileQueryTableListener extends PQLBaseListener {

        final Map<String, RecordBufferMeta> metaMap;
        /**
         * nestedInstance is the variable name used in the table definition, which is used in
         * query, different from the table name.
         *
         * <p>
         *     For example, a table definition can be:
         *
         *     table1 { col1: LONG, col2: table1 }, and
         *
         *     a query can be:
         *
         *     select col1, col2.col1
         *     from table1, table1.col2
         *
         *     where table1 is the table name, and col2 is the nested instance field name.
         *
         *     Therefore, the values of the following two variables can have:
         *
         *     col2 --> table1
         *     col2 --> field type of table1
         * </p>
         */
        Map<String, RecordBufferMeta> tableColMetaMap = new HashMap<>();
        Set<String> wheretableSet = new HashSet<>();
        String mainTable = "";

        private Set<String> tableSet = new HashSet<>();

        ProfileQueryTableListener(Map<String, RecordBufferMeta> tableMetadataMap) {
            metaMap = tableMetadataMap;
        }

        @Override
        public void exitSelect_statement(PQLParser.Select_statementContext ctx) {
            for (String tableTxt : wheretableSet) {
                // find out the main table
                String superTable = tableTxt.contains(".")
                        ? tableTxt.substring(0, tableTxt.indexOf('.'))
                        : tableTxt;

                if (mainTable.isEmpty() || mainTable.equals(superTable)) {
                    mainTable = superTable;
                    if (!tableColMetaMap.containsKey(mainTable)) {
                        // deal with the tables occurring in FROM
                        tableSet.forEach(tableName ->
                                tableColMetaMap.putAll(findNestedTableMeta(
                                        tableName.startsWith(mainTable) ?
                                                tableName : mainTable + "." + tableName))
                        );

                        // add the item of main table
                        tableColMetaMap.put(mainTable, metaMap.get(mainTable));
                    }
                } else  {
                    String msg = String.format("There has been a main table, %s", mainTable);
                    LOG.error(msg);
                    throw new IllegalArgumentException(msg);
                }

                // find out the metadata of sub tables
                tableColMetaMap.putAll(findNestedTableMeta(tableTxt));
            }

            if (tableColMetaMap.isEmpty()) {
                String msg = "There must be at least ONE table involved in the query!";
                LOG.error(msg);
                throw new IllegalArgumentException(msg);
            }
        }

        @Override
        public void exitFull_column_name(PQLParser.Full_column_nameContext ctx) {
            if (ctx.table_name() != null && !ctx.table_name().getText().isEmpty()) {
                String tableName = ctx.table_name().getText();
                tableSet.add(tableName);
            }
        }

        @Override
        public void exitTable_list(PQLParser.Table_listContext ctx) {
            if (ctx.getChildCount() > 0) {
                for (ParseTree cur : ctx.table_name()) {
                    String tableTxt = cur.getText();
                    if (StringUtils.isNotBlank(tableTxt)) {
                        wheretableSet.add(tableTxt);
                    }
                }
                LOG.debug(String.format("Tables to be queried: %s", tableColMetaMap));
            }
        }

        private Map<String, RecordBufferMeta> findNestedTableMeta(String tableInstanceName) {
            Map<String, RecordBufferMeta> ret = new HashMap<>();

            String[] tables = tableInstanceName.contains(".")
                    ? tableInstanceName.split("\\.")
                    : new String[]{tableInstanceName};

            // the left table should be always main table
            RecordBufferMeta curTableMeta = metaMap.get(tables[0]);
            if (curTableMeta == null) {
                String msg = String.format("%s does not exist!", tables[0]);
                LOG.error(msg);
                throw new IllegalArgumentException(msg);
            }

            // TODO duplicates can occur, how to improve?
            StringBuilder fullTableNameSB = new StringBuilder(tables[0]);
            for (int i = 1; i < tables.length; ++i) {
                RecordMetadataInterface fieldMeta =
                        (RecordMetadataInterface) curTableMeta.getFieldMeta(tables[i]);
                if (fieldMeta.getField() == null) {
                    LOG.error("The field doesn't exist - " + fieldMeta);
                }
                FieldType curType = fieldMeta.getField().getType();
                fullTableNameSB.append(".").append(tables[i]);
                if (curType instanceof GroupFieldType) {
                    String nestedTableName = ((GroupFieldType) curType).getGroupName();
                    curTableMeta = metaMap.get(nestedTableName);
                } else if (curType instanceof CollectionFieldType) {
                    FieldType elementType = ((CollectionFieldType) curType).getElementType();
                    if (elementType instanceof GroupFieldType) {
                        String nestedTableName = ((GroupFieldType) elementType).getGroupName();
                        curTableMeta = metaMap.get(nestedTableName);
                    }
                }
                ret.put(fullTableNameSB.toString(), curTableMeta); // TODO fix me !!!
            }

            return ret;
        }
    }
}
