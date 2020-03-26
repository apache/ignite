/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.h2.sql;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.cache.CacheException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.util.typedef.F;
import org.h2.command.Command;
import org.h2.command.CommandContainer;
import org.h2.command.CommandInterface;
import org.h2.command.Prepared;
import org.h2.command.ddl.AlterTableAddConstraint;
import org.h2.command.ddl.AlterTableAlterColumn;
import org.h2.command.ddl.CommandWithColumns;
import org.h2.command.ddl.CreateIndex;
import org.h2.command.ddl.CreateTable;
import org.h2.command.ddl.CreateTableData;
import org.h2.command.ddl.DefineCommand;
import org.h2.command.ddl.DropIndex;
import org.h2.command.ddl.DropTable;
import org.h2.command.ddl.SchemaCommand;
import org.h2.command.dml.Delete;
import org.h2.command.dml.Explain;
import org.h2.command.dml.Insert;
import org.h2.command.dml.Merge;
import org.h2.command.dml.Query;
import org.h2.command.dml.Select;
import org.h2.command.dml.SelectOrderBy;
import org.h2.command.dml.SelectUnion;
import org.h2.command.dml.Update;
import org.h2.engine.Constants;
import org.h2.engine.FunctionAlias;
import org.h2.expression.Aggregate;
import org.h2.expression.Alias;
import org.h2.expression.CompareLike;
import org.h2.expression.Comparison;
import org.h2.expression.ConditionAndOr;
import org.h2.expression.ConditionExists;
import org.h2.expression.ConditionIn;
import org.h2.expression.ConditionInConstantSet;
import org.h2.expression.ConditionInSelect;
import org.h2.expression.ConditionNot;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.expression.ExpressionList;
import org.h2.expression.Function;
import org.h2.expression.JavaFunction;
import org.h2.expression.Operation;
import org.h2.expression.Parameter;
import org.h2.expression.Subquery;
import org.h2.expression.TableFunction;
import org.h2.expression.ValueExpression;
import org.h2.index.ViewIndex;
import org.h2.jdbc.JdbcPreparedStatement;
import org.h2.result.SortOrder;
import org.h2.schema.Schema;
import org.h2.table.Column;
import org.h2.table.FunctionTable;
import org.h2.table.IndexColumn;
import org.h2.table.MetaTable;
import org.h2.table.RangeTable;
import org.h2.table.Table;
import org.h2.table.TableBase;
import org.h2.table.TableFilter;
import org.h2.table.TableView;
import org.h2.value.DataType;
import org.h2.value.Value;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperationType.AND;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperationType.BIGGER;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperationType.BIGGER_EQUAL;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperationType.EQUAL;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperationType.EQUAL_NULL_SAFE;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperationType.EXISTS;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperationType.IN;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperationType.IS_NOT_NULL;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperationType.IS_NULL;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperationType.LIKE;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperationType.NOT;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperationType.NOT_EQUAL;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperationType.NOT_EQUAL_NULL_SAFE;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperationType.OR;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperationType.REGEXP;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperationType.SMALLER;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperationType.SMALLER_EQUAL;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperationType.SPATIAL_INTERSECTS;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlType.fromColumn;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlType.fromExpression;

/**
 * H2 Query parser.
 */
public class GridSqlQueryParser {
    /** */
    private static final GridSqlOperationType[] COMPARISON_TYPES =
        {EQUAL, BIGGER_EQUAL, BIGGER, SMALLER_EQUAL,
        SMALLER, NOT_EQUAL, IS_NULL, IS_NOT_NULL,
        null, null, null, SPATIAL_INTERSECTS /* 11 */,
        null, null, null, null, EQUAL_NULL_SAFE /* 16 */,
        null, null, null, null, NOT_EQUAL_NULL_SAFE /* 21 */};

    /** */
    private static final Getter<Select, Expression> CONDITION = getter(Select.class, "condition");

    /** */
    private static final Getter<Select, int[]> GROUP_INDEXES = getter(Select.class, "groupIndex");

    /** */
    private static final Getter<Select, Boolean> SELECT_IS_FOR_UPDATE = getter(Select.class, "isForUpdate");

    /** */
    private static final Getter<Select, Boolean> SELECT_IS_GROUP_QUERY = getter(Select.class, "isGroupQuery");

    /** */
    private static final Getter<SelectUnion, Boolean> UNION_IS_FOR_UPDATE = getter(SelectUnion.class, "isForUpdate");

    /** */
    private static final Getter<Operation, Operation.OpType> OPERATION_TYPE = getter(Operation.class, "opType");

    /** */
    private static final Getter<Operation, Expression> OPERATION_LEFT = getter(Operation.class, "left");

    /** */
    private static final Getter<Operation, Expression> OPERATION_RIGHT = getter(Operation.class, "right");

    /** */
    private static final Getter<Comparison, Integer> COMPARISON_TYPE = getter(Comparison.class, "compareType");

    /** */
    private static final Getter<Comparison, Expression> COMPARISON_LEFT = getter(Comparison.class, "left");

    /** */
    private static final Getter<Comparison, Expression> COMPARISON_RIGHT = getter(Comparison.class, "right");

    /** */
    private static final Getter<ConditionAndOr, Integer> ANDOR_TYPE = getter(ConditionAndOr.class, "andOrType");

    /** */
    private static final Getter<ConditionAndOr, Expression> ANDOR_LEFT = getter(ConditionAndOr.class, "left");

    /** */
    private static final Getter<ConditionAndOr, Expression> ANDOR_RIGHT = getter(ConditionAndOr.class, "right");

    /** */
    public static final Getter<TableView, Query> VIEW_QUERY = getter(TableView.class, "viewQuery");

    /** */
    private static final Getter<TableFilter, String> ALIAS = getter(TableFilter.class, "alias");

    /** */
    private static final Getter<Select, Integer> HAVING_INDEX = getter(Select.class, "havingIndex");

    /** */
    private static final Getter<ConditionIn, Expression> LEFT_CI = getter(ConditionIn.class, "left");

    /** */
    private static final Getter<ConditionIn, List<Expression>> VALUE_LIST_CI = getter(ConditionIn.class, "valueList");

    /** */
    private static final Getter<ConditionInConstantSet, Expression> LEFT_CICS =
        getter(ConditionInConstantSet.class, "left");

    /** */
    private static final Getter<ConditionInConstantSet, List<Expression>> VALUE_LIST_CICS =
        getter(ConditionInConstantSet.class, "valueList");

    /** */
    private static final Getter<ExpressionList, Expression[]> EXPR_LIST = getter(ExpressionList.class, "list");

    /** */
    private static final Getter<ConditionInSelect, Expression> LEFT_CIS = getter(ConditionInSelect.class, "left");

    /** */
    private static final Getter<ConditionInSelect, Boolean> ALL = getter(ConditionInSelect.class, "all");

    /** */
    private static final Getter<ConditionInSelect, Integer> COMPARE_TYPE = getter(ConditionInSelect.class,
        "compareType");

    /** */
    private static final Getter<ConditionInSelect, Query> QUERY_IN = getter(ConditionInSelect.class, "query");

    /** */
    private static final Getter<ConditionExists, Query> QUERY_EXISTS = getter(ConditionExists.class, "query");

    /** */
    private static final Getter<CompareLike, Expression> LEFT = getter(CompareLike.class, "left");

    /** */
    private static final Getter<CompareLike, Expression> RIGHT = getter(CompareLike.class, "right");

    /** */
    private static final Getter<CompareLike, Expression> ESCAPE = getter(CompareLike.class, "escape");

    /** */
    private static final Getter<CompareLike, Boolean> REGEXP_CL = getter(CompareLike.class, "regexp");

    /** */
    private static final Getter<Aggregate, Boolean> DISTINCT = getter(Aggregate.class, "distinct");

    /** */
    private static final Getter<Aggregate, Aggregate.AggregateType> TYPE = getter(Aggregate.class, "type");

    /** */
    private static final Getter<Aggregate, Expression> ON = getter(Aggregate.class, "on");

    /** */
    private static final Getter<Aggregate, Expression> GROUP_CONCAT_SEPARATOR = getter(Aggregate.class,
        "groupConcatSeparator");

    /** */
    private static final Getter<Aggregate, ArrayList<SelectOrderBy>> GROUP_CONCAT_ORDER_LIST = getter(Aggregate.class,
        "groupConcatOrderList");

    /** */
    private static final Getter<RangeTable, Expression> RANGE_MIN = getter(RangeTable.class, "min");

    /** */
    private static final Getter<RangeTable, Expression> RANGE_MAX = getter(RangeTable.class, "max");

    /** */
    private static final Getter<FunctionTable, Expression> FUNC_EXPR = getter(FunctionTable.class, "functionExpr");

    /** */
    private static final Getter<TableFunction, Column[]> FUNC_TBL_COLS = getter(TableFunction.class, "columnList");

    /** */
    private static final Getter<JavaFunction, FunctionAlias> FUNC_ALIAS = getter(JavaFunction.class, "functionAlias");

    /** */
    private static final Getter<ExpressionColumn, String> SCHEMA_NAME = getter(ExpressionColumn.class, "schemaName");

    /** */
    private static final Getter<JdbcPreparedStatement, Command> COMMAND = getter(JdbcPreparedStatement.class, "command");

    /** */
    private static final Getter<SelectUnion, SortOrder> UNION_SORT = getter(SelectUnion.class, "sort");

    /** */
    private static final Getter<Explain, Prepared> EXPLAIN_COMMAND = getter(Explain.class, "command");

    /** */
    private static final Getter<Merge, Table> MERGE_TABLE = getter(Merge.class, "targetTable");

    /** */
    private static final Getter<Merge, Column[]> MERGE_COLUMNS = getter(Merge.class, "columns");

    /** */
    private static final Getter<Merge, Column[]> MERGE_KEYS = getter(Merge.class, "keys");

    /** */
    private static final Getter<Merge, List<Expression[]>> MERGE_ROWS = getter(Merge.class, "valuesExpressionList");

    /** */
    private static final Getter<Merge, Query> MERGE_QUERY = getter(Merge.class, "query");

    /** */
    private static final Getter<Insert, Table> INSERT_TABLE = getter(Insert.class, "table");

    /** */
    private static final Getter<Insert, Column[]> INSERT_COLUMNS = getter(Insert.class, "columns");

    /** */
    private static final Getter<Insert, List<Expression[]>> INSERT_ROWS = getter(Insert.class, "list");

    /** */
    private static final Getter<Insert, Query> INSERT_QUERY = getter(Insert.class, "query");

    /** */
    private static final Getter<Insert, Boolean> INSERT_DIRECT = getter(Insert.class, "insertFromSelect");

    /** */
    private static final Getter<Insert, Boolean> INSERT_SORTED = getter(Insert.class, "sortedInsertMode");

    /** */
    private static final Getter<Delete, TableFilter> DELETE_FROM = getter(Delete.class, "targetTableFilter");

    /** */
    private static final Getter<Delete, Expression> DELETE_WHERE = getter(Delete.class, "condition");

    /** */
    private static final Getter<Delete, Expression> DELETE_LIMIT = getter(Delete.class, "limitExpr");

    /** */
    private static final Getter<Update, TableFilter> UPDATE_TARGET = getter(Update.class, "targetTableFilter");

    /** */
    private static final Getter<Update, ArrayList<Column>> UPDATE_COLUMNS = getter(Update.class, "columns");

    /** */
    private static final Getter<Update, HashMap<Column, Expression>> UPDATE_SET = getter(Update.class,
        "expressionMap");

    /** */
    private static final Getter<Update, Expression> UPDATE_WHERE = getter(Update.class, "condition");

    /** */
    private static final Getter<Update, Expression> UPDATE_LIMIT = getter(Update.class, "limitExpr");

    /** */
    private static final Getter<Command, Prepared> PREPARED =
        GridSqlQueryParser.<Command, Prepared>getter(CommandContainer.class, "prepared");

    /** */
    private static final Getter<CreateIndex, String> CREATE_INDEX_NAME = getter(CreateIndex.class, "indexName");

    /** */
    private static final Getter<CreateIndex, String> CREATE_INDEX_TABLE_NAME = getter(CreateIndex.class, "tableName");

    /** */
    private static final Getter<CreateIndex, IndexColumn[]> CREATE_INDEX_COLUMNS = getter(CreateIndex.class,
        "indexColumns");

    /** */
    private static final Getter<CreateIndex, Boolean> CREATE_INDEX_SPATIAL = getter(CreateIndex.class, "spatial");

    /** */
    private static final Getter<CreateIndex, Boolean> CREATE_INDEX_PRIMARY_KEY = getter(CreateIndex.class,
        "primaryKey");

    /** */
    private static final Getter<CreateIndex, Boolean> CREATE_INDEX_UNIQUE = getter(CreateIndex.class, "unique");

    /** */
    private static final Getter<CreateIndex, Boolean> CREATE_INDEX_HASH = getter(CreateIndex.class, "hash");

    /** */
    private static final Getter<CreateIndex, Boolean> CREATE_INDEX_IF_NOT_EXISTS = getter(CreateIndex.class,
        "ifNotExists");

    /** */
    private static final Getter<IndexColumn, String> INDEX_COLUMN_NAME = getter(IndexColumn.class, "columnName");

    /** */
    private static final Getter<IndexColumn, Integer> INDEX_COLUMN_SORT_TYPE = getter(IndexColumn.class, "sortType");

    /** */
    private static final Getter<DropIndex, String> DROP_INDEX_NAME = getter(DropIndex.class, "indexName");

    /** */
    private static final Getter<DropIndex, Boolean> DROP_INDEX_IF_EXISTS = getter(DropIndex.class, "ifExists");

    /** */
    private static final Getter<SchemaCommand, Schema> SCHEMA_COMMAND_SCHEMA = getter(SchemaCommand.class, "schema");

    /** */
    private static final Getter<CreateTable, CreateTableData> CREATE_TABLE_DATA = getter(CreateTable.class, "data");

    /** */
    private static final Getter<CommandWithColumns, ArrayList<DefineCommand>> CREATE_TABLE_CONSTRAINTS =
        getter(CommandWithColumns.class, "constraintCommands");

    /** */
    private static final Getter<CommandWithColumns, IndexColumn[]> CREATE_TABLE_PK =
        getter(CommandWithColumns.class, "pkColumns");

    /** */
    private static final Getter<CreateTable, Boolean> CREATE_TABLE_IF_NOT_EXISTS = getter(CreateTable.class,
        "ifNotExists");

    /** */
    private static final Getter<CreateTable, Query> CREATE_TABLE_QUERY = getter(CreateTable.class, "asQuery");

    /** */
    private static final Getter<DropTable, Boolean> DROP_TABLE_IF_EXISTS = getter(DropTable.class, "ifExists");

    /** */
    private static final Getter<DropTable, String> DROP_TABLE_NAME = getter(DropTable.class, "tableName");

    /** */
    private static final Getter<Column, Boolean> COLUMN_IS_COMPUTED = getter(Column.class, "isComputed");

    /** */
    private static final Getter<Column, Expression> COLUMN_CHECK_CONSTRAINT = getter(Column.class, "checkConstraint");

    /** Class for private class: 'org.h2.command.CommandList'. */
    private static final Class<? extends Command> CLS_COMMAND_LIST;

    /** */
    private static final Getter<Command, Command> LIST_COMMAND;

    /** */
    private static final Getter<Command, String> REMAINING;

    /** */
    public static final String ORG_H2_COMMAND_COMMAND_LIST = "org.h2.command.CommandList";

    static {
        try {
            CLS_COMMAND_LIST = (Class<? extends Command>)CommandContainer.class.getClassLoader()
                .loadClass(ORG_H2_COMMAND_COMMAND_LIST);

            LIST_COMMAND = getter(CLS_COMMAND_LIST, "command");

            REMAINING = getter(CLS_COMMAND_LIST, "remaining");
        }
        catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    /** */
    private static final Getter<AlterTableAlterColumn, String> ALTER_COLUMN_TBL_NAME =
        getter(AlterTableAlterColumn.class, "tableName");

    /** */
    private static final Getter<AlterTableAlterColumn, ArrayList<Column>> ALTER_COLUMN_NEW_COLS =
        getter(AlterTableAlterColumn.class, "columnsToAdd");

    /** */
    private static final Getter<AlterTableAlterColumn, ArrayList<Column>> ALTER_COLUMN_REMOVE_COLS =
        getter(AlterTableAlterColumn.class, "columnsToRemove");

    /** */
    private static final Getter<AlterTableAlterColumn, Boolean> ALTER_COLUMN_IF_NOT_EXISTS =
        getter(AlterTableAlterColumn.class, "ifNotExists");

    /** */
    private static final Getter<AlterTableAlterColumn, Boolean> ALTER_COLUMN_IF_TBL_EXISTS =
        getter(AlterTableAlterColumn.class, "ifTableExists");

    /** */
    private static final Getter<AlterTableAlterColumn, String> ALTER_COLUMN_BEFORE_COL =
        getter(AlterTableAlterColumn.class, "addBefore");

    /** */
    private static final Getter<AlterTableAlterColumn, Boolean> ALTER_COLUMN_FIRST =
        getter(AlterTableAlterColumn.class, "addFirst");

    /** */
    private static final Getter<AlterTableAlterColumn, String> ALTER_COLUMN_AFTER_COL =
        getter(AlterTableAlterColumn.class, "addAfter");

    /** */
    private static final String PARAM_NAME_VALUE_SEPARATOR = "=";

    /** */
    private static final String PARAM_TEMPLATE = "TEMPLATE";

    /** */
    private static final String PARAM_BACKUPS = "BACKUPS";

    /** */
    private static final String PARAM_ATOMICITY = "ATOMICITY";

    /** */
    private static final String PARAM_CACHE_GROUP_OLD = "CACHEGROUP";

    /** */
    private static final String PARAM_AFFINITY_KEY_OLD = "AFFINITYKEY";

    /** */
    private static final String PARAM_CACHE_GROUP = "CACHE_GROUP";

    /** */
    private static final String PARAM_AFFINITY_KEY = "AFFINITY_KEY";

    /** */
    private static final String PARAM_WRITE_SYNC = "WRITE_SYNCHRONIZATION_MODE";

    /** */
    private static final String PARAM_CACHE_NAME = "CACHE_NAME";

    /** */
    private static final String PARAM_KEY_TYPE = "KEY_TYPE";

    /** */
    private static final String PARAM_VAL_TYPE = "VALUE_TYPE";

    /** */
    private static final String PARAM_WRAP_KEY = "WRAP_KEY";

    /** */
    public static final String PARAM_WRAP_VALUE = "WRAP_VALUE";

    /** Data region name. */
    public static final String PARAM_DATA_REGION = "DATA_REGION";

    /** */
    private static final String PARAM_ENCRYPTED = "ENCRYPTED";

    /** Query parallelism value of cache configuration. */
    private static final String PARAM_PARALLELISM = "PARALLELISM";

    /** */
    private final IdentityHashMap<Object, Object> h2ObjToGridObj = new IdentityHashMap<>();

    /** */
    private final Map<String, Integer> optimizedTableFilterOrder;

    /** */
    private final IgniteLogger log;

    /**
     * We have a counter instead of a simple flag, because
     * a flag can be reset earlier than needed in case of
     * deep subquery expression nesting.
     */
    private int parsingSubQryExpression;

    /** Whether this is SELECT FOR UPDATE. */
    private boolean selectForUpdate;

    /**
     * @param useOptimizedSubqry If we have to find correct order for table filters in FROM clause.
     *                           Relies on uniqueness of table filter aliases.
     * @param log Logger.
     */
    public GridSqlQueryParser(boolean useOptimizedSubqry, IgniteLogger log) {
        assert Objects.nonNull(log);

        optimizedTableFilterOrder = useOptimizedSubqry ? new HashMap<>() : null;

        this.log = log;
    }

    /**
     * @param stmt Prepared statement to check.
     * @return {@code true} in case of multiple statements.
     */
    public static boolean checkMultipleStatements(PreparedStatement stmt) {
        Command cmd = extractCommand(stmt);

        return ORG_H2_COMMAND_COMMAND_LIST.equals(cmd.getClass().getName());
    }

    /**
     * @param stmt Prepared statement.
     * @return Parsed select.
     */
    public static Prepared prepared(PreparedStatement stmt) {
        Command cmd = extractCommand(stmt);

        assert cmd instanceof CommandContainer;

        return PREPARED.get(cmd);
    }

    /**
     * @param stmt Prepared statement.
     * @return Parsed select.
     */
    public static PreparedWithRemaining preparedWithRemaining(PreparedStatement stmt) {
        Command cmd = extractCommand(stmt);

        if (cmd instanceof CommandContainer)
            return new PreparedWithRemaining(PREPARED.get(cmd), null);
        else {
            Class<?> cmdCls = cmd.getClass();

            if (cmdCls.getName().equals(ORG_H2_COMMAND_COMMAND_LIST))
                return new PreparedWithRemaining(PREPARED.get(LIST_COMMAND.get(cmd)), REMAINING.get(cmd));
            else
                throw new IgniteSQLException("Unexpected statement command");
        }
    }

    /** */
    private static Command extractCommand(PreparedStatement stmt) {
        return COMMAND.get((JdbcPreparedStatement)stmt);
    }

    /**
     * @param p Prepared.
     * @return Whether {@code p} is an {@code SELECT FOR UPDATE} query.
     */
    public static boolean isForUpdateQuery(Prepared p) {
        boolean union;

        if (p.getClass() == Select.class)
            union = false;
        else if (p.getClass() == SelectUnion.class)
            union = true;
        else
            return false;

        boolean forUpdate = (!union && SELECT_IS_FOR_UPDATE.get((Select)p)) ||
            (union && UNION_IS_FOR_UPDATE.get((SelectUnion)p));

        if (union && forUpdate) {
            throw new IgniteSQLException("SELECT UNION FOR UPDATE is not supported.",
                IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
        }

        return forUpdate;
    }

    /**
     * @param qry Query expression to parse.
     * @return Subquery AST.
     */
    private GridSqlSubquery parseQueryExpression(Query qry) {
        parsingSubQryExpression++;
        GridSqlQuery subQry = parseQuery(qry);
        parsingSubQryExpression--;

        return new GridSqlSubquery(subQry);
    }

    /**
     * @param filter Filter.
     */
    private GridSqlElement parseTableFilter(TableFilter filter) {
        GridSqlElement res = (GridSqlElement)h2ObjToGridObj.get(filter);

        if (res == null) {
            res = parseTable(filter.getTable());

            // Setup index hints.
            if (res instanceof GridSqlTable && filter.getIndexHints() != null)
                ((GridSqlTable)res).useIndexes(new ArrayList<>(filter.getIndexHints().getAllowedIndexes()));

            String alias = ALIAS.get(filter);

            if (alias != null)
                res = new GridSqlAlias(alias, res, false);

            h2ObjToGridObj.put(filter, res);
        }

        return res;
    }

    /**
     * @param tbl Table.
     */
    private GridSqlElement parseTable(Table tbl) {
        GridSqlElement res = (GridSqlElement)h2ObjToGridObj.get(tbl);

        if (res == null) {
            // We can't cache simple tables because otherwise it will be the same instance for all
            // table filters. Thus we will not be able to distinguish one table filter from another.
            // Table here is semantically equivalent to a table filter.
            if (tbl instanceof TableBase || tbl instanceof MetaTable)
                return new GridSqlTable(tbl);

            // Other stuff can be cached because we will have separate instances in
            // different table filters anyways. Thus the semantics will be correct.
            if (tbl instanceof TableView) {
                if (((TableView)tbl).isRecursive()) {
                    throw new IgniteSQLException("Recursive CTE ('WITH RECURSIVE (...)') is not supported.",
                        IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
                }

                Query qry = VIEW_QUERY.get((TableView) tbl);

                res = new GridSqlSubquery(parseQuery(qry));
            }
            else if (tbl instanceof FunctionTable)
                res = parseExpression(FUNC_EXPR.get((FunctionTable)tbl), false);
            else if (tbl instanceof RangeTable) {
                res = new GridSqlFunction(GridSqlFunctionType.SYSTEM_RANGE);

                res.addChild(parseExpression(RANGE_MIN.get((RangeTable)tbl), false));
                res.addChild(parseExpression(RANGE_MAX.get((RangeTable)tbl), false));
            }
            else if (tbl instanceof MetaTable)
                res = new GridSqlTable(tbl);
            else
                assert0(false, "Unexpected Table implementation [cls=" + tbl.getClass().getSimpleName() + ']');

            h2ObjToGridObj.put(tbl, res);
        }

        return res;
    }

    /**
     * @param select Select.
     */
    private GridSqlSelect parseSelect(Select select) {
        GridSqlSelect res = (GridSqlSelect)h2ObjToGridObj.get(select);

        if (res != null)
            return res;

        res = new GridSqlSelect();

        h2ObjToGridObj.put(select, res);

        res.distinct(select.isDistinct());

        Expression where = CONDITION.get(select);
        res.where(parseExpression(where, true));

        ArrayList<TableFilter> tableFilters = new ArrayList<>();

        TableFilter filter = select.getTopTableFilter();

        boolean isForUpdate = SELECT_IS_FOR_UPDATE.get(select);

        do {
            assert0(filter != null, select);
            assert0(filter.getNestedJoin() == null, select);

            // Can use optimized join order only if we are not inside of an expression.
            if (parsingSubQryExpression == 0 && optimizedTableFilterOrder != null) {
                String tblAlias = filter.getTableAlias();
                int idx = optimizedTableFilterOrder.get(tblAlias);

                setElementAt(tableFilters, idx, filter);
            }
            else
                tableFilters.add(filter);

            filter = filter.getJoin();
        }
        while (filter != null);

        // Build FROM clause from correctly ordered table filters.
        GridSqlElement from = null;

        for (int i = 0; i < tableFilters.size(); i++) {
            TableFilter f = tableFilters.get(i);
            GridSqlElement gridFilter = parseTableFilter(f);

            from = from == null ? gridFilter : new GridSqlJoin(from, gridFilter, f.isJoinOuter(),
                parseExpression(f.getJoinCondition(), true));
        }

        res.from(from);

        if (isForUpdate) {
            if (!(from instanceof GridSqlTable ||
                (from instanceof GridSqlAlias && from.size() == 1 && from.child() instanceof GridSqlTable))) {
                throw new IgniteSQLException("SELECT FOR UPDATE with joins is not supported.",
                    IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
            }

            GridSqlTable gridTbl = from instanceof GridSqlTable ? (GridSqlTable)from :
                ((GridSqlAlias)from).child();

            GridH2Table tbl = gridTbl.dataTable();

            if (tbl == null) {
                throw new IgniteSQLException("SELECT FOR UPDATE query must involve Ignite table.",
                    IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
            }

            if (select.getLimit() != null || select.getOffset() != null) {
                throw new IgniteSQLException("LIMIT/OFFSET clauses are not supported for SELECT FOR UPDATE.",
                    IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
            }

            if (SELECT_IS_GROUP_QUERY.get(select)) {
                throw new IgniteSQLException("SELECT FOR UPDATE with aggregates and/or GROUP BY is not supported.",
                    IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
            }

            if (select.isDistinct())
                throw new IgniteSQLException("DISTINCT clause is not supported for SELECT FOR UPDATE.",
                    IgniteQueryErrorCode.UNSUPPORTED_OPERATION);

            if (SplitterUtils.hasSubQueries(res))
                throw new IgniteSQLException("Sub queries are not supported for SELECT FOR UPDATE.",
                    IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
        }

        ArrayList<Expression> expressions = select.getExpressions();

        for (int i = 0; i < expressions.size(); i++)
            res.addColumn(parseExpression(expressions.get(i), true), i < select.getColumnCount());

        int[] grpIdx = GROUP_INDEXES.get(select);

        if (grpIdx != null)
            res.groupColumns(grpIdx);

        int havingIdx = HAVING_INDEX.get(select);

        if (havingIdx >= 0)
            res.havingColumn(havingIdx);

        res.forUpdate(isForUpdate);

        processSortOrder(select.getSortOrder(), res);

        res.limit(parseExpression(select.getLimit(), false));
        res.offset(parseExpression(select.getOffset(), false));

        return res;
    }

    /**
     * @param list List.
     * @param idx Index.
     * @param x Element.
     */
    private static <Z> void setElementAt(List<Z> list, int idx, Z x) {
        while (list.size() <= idx)
            list.add(null);

        assert0(list.get(idx) == null, "Element already set: " + idx);

        list.set(idx, x);
    }

    /**
     * @param merge Merge.
     * @see <a href="http://h2database.com/html/grammar.html#merge">H2 merge spec</a>
     */
    private GridSqlMerge parseMerge(Merge merge) {
        GridSqlMerge res = (GridSqlMerge)h2ObjToGridObj.get(merge);

        if (res != null)
            return res;

        res = new GridSqlMerge();
        h2ObjToGridObj.put(merge, res);

        Table srcTbl = MERGE_TABLE.get(merge);
        GridSqlElement tbl = parseTable(srcTbl);

        res.into(tbl);

        Column[] srcCols = MERGE_COLUMNS.get(merge);

        GridSqlColumn[] cols = new GridSqlColumn[srcCols.length];

        for (int i = 0; i < srcCols.length; i++) {
            cols[i] = new GridSqlColumn(srcCols[i], tbl, null, null, srcCols[i].getName());

            cols[i].resultType(fromColumn(srcCols[i]));
        }

        res.columns(cols);

        if (!F.isEmpty(MERGE_KEYS.get(merge))) {
            log.warning("The search row by explicit KEY isn't supported. The primary key is always used to search row " +
                "[sql=" + merge.getSQL() + ']');
        }

        List<Expression[]> srcRows = MERGE_ROWS.get(merge);
        if (!srcRows.isEmpty()) {
            List<GridSqlElement[]> rows = new ArrayList<>(srcRows.size());
            for (Expression[] srcRow : srcRows) {
                GridSqlElement[] row = new GridSqlElement[srcRow.length];

                for (int i = 0; i < srcRow.length; i++) {
                    row[i] = parseExpression(srcRow[i], false);

                    if (row[i] == null) {
                        throw new IgniteSQLException("Explicit DEFAULT values are unsupported for MERGE.",
                            IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
                    }
                }

                rows.add(row);
            }
            res.rows(rows);
        }
        else {
            res.rows(Collections.emptyList());
            res.query(parseQuery(MERGE_QUERY.get(merge)));
        }

        return res;
    }

    /**
     * @param insert Insert.
     * @see <a href="http://h2database.com/html/grammar.html#insert">H2 insert spec</a>
     */
    private GridSqlInsert parseInsert(Insert insert) {
        GridSqlInsert res = (GridSqlInsert)h2ObjToGridObj.get(insert);

        if (res != null)
            return res;

        res = new GridSqlInsert();
        h2ObjToGridObj.put(insert, res);

        Table srcTbl = INSERT_TABLE.get(insert);
        GridSqlElement tbl = parseTable(srcTbl);

        res.into(tbl).
            direct(INSERT_DIRECT.get(insert)).
            sorted(INSERT_SORTED.get(insert));

        Column[] srcCols = INSERT_COLUMNS.get(insert);
        GridSqlColumn[] cols = new GridSqlColumn[srcCols.length];

        for (int i = 0; i < srcCols.length; i++) {
            cols[i] = new GridSqlColumn(srcCols[i], tbl, null, null, srcCols[i].getName());

            cols[i].resultType(fromColumn(srcCols[i]));
        }

        res.columns(cols);

        List<Expression[]> srcRows = INSERT_ROWS.get(insert);
        if (!srcRows.isEmpty()) {
            List<GridSqlElement[]> rows = new ArrayList<>(srcRows.size());
            for (Expression[] srcRow : srcRows) {
                GridSqlElement[] row = new GridSqlElement[srcRow.length];

                for (int i = 0; i < srcRow.length; i++) {
                    row[i] = parseExpression(srcRow[i], false);

                    if (row[i] == null) {
                        throw new IgniteSQLException("Explicit DEFAULT values are unsupported for INSERT.",
                            IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
                    }
                }

                rows.add(row);
            }
            res.rows(rows);
        }
        else {
            res.rows(Collections.<GridSqlElement[]>emptyList());
            res.query(parseQuery(INSERT_QUERY.get(insert)));
        }

        return res;
    }

    /**
     * @param del Delete.
     * @see <a href="http://h2database.com/html/grammar.html#delete">H2 delete spec</a>
     */
    private GridSqlDelete parseDelete(Delete del) {
        GridSqlDelete res = (GridSqlDelete)h2ObjToGridObj.get(del);

        if (res != null)
            return res;

        res = new GridSqlDelete();
        h2ObjToGridObj.put(del, res);

        GridSqlElement tbl = parseTableFilter(DELETE_FROM.get(del));
        GridSqlElement where = parseExpression(DELETE_WHERE.get(del), true);
        GridSqlElement limit = parseExpression(DELETE_LIMIT.get(del), true);
        res.from(tbl).where(where).limit(limit);
        return res;
    }

    /**
     * @param update Update.
     * @see <a href="http://h2database.com/html/grammar.html#update">H2 update spec</a>
     */
    private GridSqlUpdate parseUpdate(Update update) {
        GridSqlUpdate res = (GridSqlUpdate)h2ObjToGridObj.get(update);

        if (res != null)
            return res;

        res = new GridSqlUpdate();
        h2ObjToGridObj.put(update, res);

        GridSqlElement tbl = parseTableFilter(UPDATE_TARGET.get(update));

        List<Column> srcCols = UPDATE_COLUMNS.get(update);
        Map<Column, Expression> srcSet = UPDATE_SET.get(update);

        ArrayList<GridSqlColumn> cols = new ArrayList<>(srcCols.size());
        LinkedHashMap<String, GridSqlElement> set = new LinkedHashMap<>(srcSet.size());

        for (Column c : srcCols) {
            GridSqlColumn col = new GridSqlColumn(c, tbl, null, null, c.getName());
            col.resultType(fromColumn(c));
            cols.add(col);

            GridSqlElement setVal = parseExpression(srcSet.get(c), true);

            if (containsDefaultKeyword(setVal)) {
                throw new IgniteSQLException("DEFAULT values are unsupported for UPDATE.",
                    IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
            }

            set.put(col.columnName(), setVal);
        }

        GridSqlElement where = parseExpression(UPDATE_WHERE.get(update), true);
        GridSqlElement limit = parseExpression(UPDATE_LIMIT.get(update), true);

        res.target(tbl).cols(cols).set(set).where(where).limit(limit);
        return res;
    }

    /**
     * @param val SQL expression.
     * @return {@code true} if the expression contains DEFAULT keyword.
     */
    private boolean containsDefaultKeyword(GridSqlAst val) {
        if (val == GridSqlKeyword.DEFAULT)
            return true;

        for (int i = 0; i < val.size(); ++i) {
            if (containsDefaultKeyword(val.child(i)))
                return true;
        }

        return false;
    }

    /**
     * Parse {@code DROP INDEX} statement.
     *
     * @param dropIdx {@code DROP INDEX} statement.
     * @see <a href="http://h2database.com/html/grammar.html#drop_index">H2 {@code DROP INDEX} spec.</a>
     */
    private GridSqlDropIndex parseDropIndex(DropIndex dropIdx) {
        GridSqlDropIndex res = new GridSqlDropIndex();

        res.indexName(DROP_INDEX_NAME.get(dropIdx));
        res.schemaName(SCHEMA_COMMAND_SCHEMA.get(dropIdx).getName());
        res.ifExists(DROP_INDEX_IF_EXISTS.get(dropIdx));

        return res;
    }

    /**
     * Parse {@code CREATE INDEX} statement.
     *
     * @param createIdx {@code CREATE INDEX} statement.
     * @see <a href="http://h2database.com/html/grammar.html#create_index">H2 {@code CREATE INDEX} spec.</a>
     */
    private GridSqlCreateIndex parseCreateIndex(CreateIndex createIdx) {
        if (CREATE_INDEX_HASH.get(createIdx) || CREATE_INDEX_PRIMARY_KEY.get(createIdx) ||
            CREATE_INDEX_UNIQUE.get(createIdx)) {
            throw new IgniteSQLException("Only SPATIAL modifier is supported for CREATE INDEX",
                IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
        }

        GridSqlCreateIndex res = new GridSqlCreateIndex();

        Schema schema = SCHEMA_COMMAND_SCHEMA.get(createIdx);

        String tblName = CREATE_INDEX_TABLE_NAME.get(createIdx);

        res.schemaName(schema.getName());
        res.tableName(tblName);
        res.ifNotExists(CREATE_INDEX_IF_NOT_EXISTS.get(createIdx));

        QueryIndex idx = new QueryIndex();

        idx.setName(CREATE_INDEX_NAME.get(createIdx));
        idx.setIndexType(CREATE_INDEX_SPATIAL.get(createIdx) ? QueryIndexType.GEOSPATIAL : QueryIndexType.SORTED);

        IndexColumn[] cols = CREATE_INDEX_COLUMNS.get(createIdx);

        LinkedHashMap<String, Boolean> flds = new LinkedHashMap<>(cols.length);

        for (IndexColumn col : CREATE_INDEX_COLUMNS.get(createIdx)) {
            int sortType = INDEX_COLUMN_SORT_TYPE.get(col);

            if ((sortType & SortOrder.NULLS_FIRST) != 0 || (sortType & SortOrder.NULLS_LAST) != 0) {
                throw new IgniteSQLException("NULLS FIRST and NULLS LAST modifiers are not supported for index columns",
                    IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
            }

            flds.put(INDEX_COLUMN_NAME.get(col), (sortType & SortOrder.DESCENDING) == 0);
        }

        idx.setFields(flds);

        res.index(idx);

        return res;
    }

    /**
     * Parse {@code CREATE TABLE} statement.
     *
     * @param createTbl {@code CREATE TABLE} statement.
     * @see <a href="http://h2database.com/html/grammar.html#create_table">H2 {@code CREATE TABLE} spec.</a>
     */
    private GridSqlCreateTable parseCreateTable(CreateTable createTbl) {
        GridSqlCreateTable res = new GridSqlCreateTable();

        res.templateName(QueryUtils.TEMPLATE_PARTITIONED);

        Query qry = CREATE_TABLE_QUERY.get(createTbl);

        if (qry != null) {
            throw new IgniteSQLException("CREATE TABLE ... AS ... syntax is not supported",
                IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
        }

        List<DefineCommand> constraints = CREATE_TABLE_CONSTRAINTS.get(createTbl);

        if (F.isEmpty(constraints)) {
            throw new IgniteSQLException("No PRIMARY KEY defined for CREATE TABLE",
                IgniteQueryErrorCode.PARSING);
        }

        if (constraints.size() > 1) {
            throw new IgniteSQLException("Too many constraints - only PRIMARY KEY is supported for CREATE TABLE",
                IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
        }

        DefineCommand constraint = constraints.get(0);

        if (!(constraint instanceof AlterTableAddConstraint)) {
            throw new IgniteSQLException("Unsupported type of constraint for CREATE TABLE - only PRIMARY KEY " +
                "is supported", IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
        }

        AlterTableAddConstraint alterTbl = (AlterTableAddConstraint)constraint;

        if (alterTbl.getType() != Command.ALTER_TABLE_ADD_CONSTRAINT_PRIMARY_KEY) {
            throw new IgniteSQLException("Unsupported type of constraint for CREATE TABLE - only PRIMARY KEY " +
                "is supported", IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
        }

        Schema schema = SCHEMA_COMMAND_SCHEMA.get(createTbl);

        res.schemaName(schema.getName());

        CreateTableData data = CREATE_TABLE_DATA.get(createTbl);

        if (data.globalTemporary) {
            throw new IgniteSQLException("GLOBAL TEMPORARY keyword is not supported",
                IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
        }

        if (data.temporary) {
            throw new IgniteSQLException("TEMPORARY keyword is not supported",
                IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
        }

        if (data.isHidden) {
            throw new IgniteSQLException("HIDDEN keyword is not supported",
                IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
        }

        if (!data.persistIndexes) {
            throw new IgniteSQLException("MEMORY and NOT PERSISTENT keywords are not supported",
                IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
        }

        LinkedHashMap<String, GridSqlColumn> cols = new LinkedHashMap<>(data.columns.size());

        for (Column col : data.columns) {
            if (cols.put(col.getName(), parseColumn(col)) != null)
                throw new IgniteSQLException("Duplicate column name: " + col.getName(), IgniteQueryErrorCode.PARSING);
        }

        if (cols.containsKey(QueryUtils.KEY_FIELD_NAME.toUpperCase()) ||
            cols.containsKey(QueryUtils.VAL_FIELD_NAME.toUpperCase())) {
            throw new IgniteSQLException("Direct specification of _KEY and _VAL columns is forbidden",
                IgniteQueryErrorCode.PARSING);
        }

        IndexColumn[] pkIdxCols = CREATE_TABLE_PK.get(createTbl);

        if (F.isEmpty(pkIdxCols))
            throw new AssertionError("No PRIMARY KEY columns specified");

        LinkedHashSet<String> pkCols = new LinkedHashSet<>();

        for (IndexColumn pkIdxCol : pkIdxCols) {
            GridSqlColumn gridCol = cols.get(pkIdxCol.columnName);

            if (gridCol == null) {
                throw new IgniteSQLException("PRIMARY KEY column is not defined: " + pkIdxCol.columnName,
                    IgniteQueryErrorCode.PARSING);
            }

            pkCols.add(gridCol.columnName());
        }

        int keyColsNum = pkCols.size();
        int valColsNum = cols.size() - keyColsNum;

        if (valColsNum == 0) {
            throw new IgniteSQLException("Table must have at least one non PRIMARY KEY column.",
                IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
        }

        res.columns(cols);
        res.primaryKeyColumns(pkCols);
        res.tableName(data.tableName);
        res.ifNotExists(CREATE_TABLE_IF_NOT_EXISTS.get(createTbl));

        List<String> extraParams = data.tableEngineParams != null ? new ArrayList<String>() : null;

        if (data.tableEngineParams != null)
            for (String s : data.tableEngineParams)
                extraParams.addAll(F.asList(s.split(",")));

        res.params(extraParams);

        if (!F.isEmpty(extraParams)) {
            Map<String, String> params = new HashMap<>();

            for (String p : extraParams) {
                String[] parts = p.split(PARAM_NAME_VALUE_SEPARATOR);

                if (parts.length > 2) {
                    throw new IgniteSQLException("Invalid parameter (key[=value] expected): " + p,
                        IgniteQueryErrorCode.PARSING);
                }

                String name = parts[0].trim().toUpperCase();

                String val = parts.length > 1 ? parts[1].trim() : null;

                if (F.isEmpty(name)) {
                    throw new IgniteSQLException("Invalid parameter (key[=value] expected): " + p,
                        IgniteQueryErrorCode.PARSING);
                }

                if (params.put(name, val) != null)
                    throw new IgniteSQLException("Duplicate parameter: " + p, IgniteQueryErrorCode.PARSING);
            }

            for (Map.Entry<String, String> e : params.entrySet())
                processExtraParam(e.getKey(), e.getValue(), res);
        }

        // Process key wrapping.
        Boolean wrapKey = res.wrapKey();

        if (wrapKey != null && !wrapKey) {
            if (keyColsNum > 1) {
                throw new IgniteSQLException(PARAM_WRAP_KEY + " cannot be false when composite primary key exists.",
                    IgniteQueryErrorCode.PARSING);
            }

            if (!F.isEmpty(res.keyTypeName())) {
                throw new IgniteSQLException(PARAM_WRAP_KEY + " cannot be false when " + PARAM_KEY_TYPE + " is set.",
                    IgniteQueryErrorCode.PARSING);
            }
        }

        boolean wrapKey0 = (res.wrapKey() != null && res.wrapKey()) || !F.isEmpty(res.keyTypeName()) || keyColsNum > 1;

        res.wrapKey(wrapKey0);

        // Process value wrapping.
        Boolean wrapVal = res.wrapValue();

        if (wrapVal != null && !wrapVal) {
            if (valColsNum > 1) {
                throw new IgniteSQLException(PARAM_WRAP_VALUE + " cannot be false when multiple non-primary key " +
                    "columns exist.", IgniteQueryErrorCode.PARSING);
            }

            if (!F.isEmpty(res.valueTypeName())) {
                throw new IgniteSQLException(PARAM_WRAP_VALUE + " cannot be false when " + PARAM_VAL_TYPE + " is set.",
                    IgniteQueryErrorCode.PARSING);
            }

            res.wrapValue(false);
        }
        else
            res.wrapValue(true); // By default value is always wrapped to allow for ALTER TABLE ADD COLUMN commands.

        if (!F.isEmpty(res.valueTypeName()) && F.eq(res.keyTypeName(), res.valueTypeName())) {
            throw new IgniteSQLException("Key and value type names " +
                "should be different for CREATE TABLE: " + res.valueTypeName(), IgniteQueryErrorCode.PARSING);
        }

        if (res.affinityKey() == null) {
            LinkedHashSet<String> pkCols0 = res.primaryKeyColumns();

            if (!F.isEmpty(pkCols0) && pkCols0.size() == 1 && wrapKey0)
                res.affinityKey(pkCols0.iterator().next());
        }

        return res;
    }

    /**
     * Parse {@code DROP TABLE} statement.
     *
     * @param dropTbl {@code DROP TABLE} statement.
     * @see <a href="http://h2database.com/html/grammar.html#drop_table">H2 {@code DROP TABLE} spec.</a>
     */
    private GridSqlDropTable parseDropTable(DropTable dropTbl) {
        GridSqlDropTable res = new GridSqlDropTable();

        Schema schema = SCHEMA_COMMAND_SCHEMA.get(dropTbl);

        res.schemaName(schema.getName());

        res.ifExists(DROP_TABLE_IF_EXISTS.get(dropTbl));

        res.tableName(DROP_TABLE_NAME.get(dropTbl));

        return res;
    }

    /**
     * Parse {@code ALTER TABLE} statement.
     * @param stmt H2 statement.
     */
    private GridSqlStatement parseAlterColumn(AlterTableAlterColumn stmt) {
        switch (stmt.getType()) {
            case CommandInterface.ALTER_TABLE_ADD_COLUMN:
                return parseAddColumn(stmt);

            case CommandInterface.ALTER_TABLE_DROP_COLUMN:
                return parseDropColumn(stmt);

            default: {
                String stmtName = null;

                switch (stmt.getType()) {
                    case CommandInterface.ALTER_TABLE_ALTER_COLUMN_CHANGE_TYPE:
                    case CommandInterface.ALTER_TABLE_ALTER_COLUMN_DEFAULT:
                    case CommandInterface.ALTER_TABLE_ALTER_COLUMN_NOT_NULL:
                    case CommandInterface.ALTER_TABLE_ALTER_COLUMN_RENAME:
                    case CommandInterface.ALTER_TABLE_ALTER_COLUMN_NULL:
                    case CommandInterface.ALTER_TABLE_ALTER_COLUMN_SELECTIVITY:
                    case CommandInterface.ALTER_TABLE_ALTER_COLUMN_VISIBILITY:
                        stmtName = "ALTER COLUMN";

                        break;
                }

                if (stmtName == null) {
                    throw new IgniteSQLException("Unsupported operation: " + stmt.getSQL(),
                        IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
                }
                else {
                    throw new IgniteSQLException(stmtName + " is not supported",
                        IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
                }
            }
        }
    }

    /**
     * Turn H2 column to grid column and check requested features.
     * @param col H2 column.
     * @return Grid column.
     */
    private static GridSqlColumn parseColumn(Column col) {
        if (col.isAutoIncrement()) {
            throw new IgniteSQLException("AUTO_INCREMENT columns are not supported [colName=" + col.getName() + ']',
                IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
        }

        if (COLUMN_IS_COMPUTED.get(col)) {
            throw new IgniteSQLException("Computed columns are not supported [colName=" + col.getName() + ']',
                IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
        }

        checkTypeSupported(col.getType(), "[colName=" + col.getName() + ']');

        if (col.getDefaultExpression() != null) {
            if (!col.getDefaultExpression().isConstant()) {
                throw new IgniteSQLException("Non-constant DEFAULT expressions are not supported [colName=" + col.getName() + ']',
                    IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
            }

            DataType colType = DataType.getDataType(col.getType());
            DataType dfltType = DataType.getDataType(col.getDefaultExpression().getType());

            if ((DataType.isStringType(colType.type) && !DataType.isStringType(dfltType.type))
                || (DataType.supportsAdd(colType.type) && !DataType.supportsAdd(dfltType.type))) {
                throw new IgniteSQLException("Invalid default value for column. [colName=" + col.getName()
                    + ", colType=" + colType.name
                    + ", dfltValueType=" + dfltType.name + ']',
                    IgniteQueryErrorCode.UNEXPECTED_ELEMENT_TYPE);
            }
        }

        if (col.getSequence() != null)
            throw new IgniteSQLException("SEQUENCE columns are not supported [colName=" + col.getName() + ']',
                IgniteQueryErrorCode.UNSUPPORTED_OPERATION);

        if (col.getSelectivity() != Constants.SELECTIVITY_DEFAULT) {
            throw new IgniteSQLException("SELECTIVITY column attribute is not supported [colName=" + col.getName() + ']',
                IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
        }

        if (COLUMN_CHECK_CONSTRAINT.get(col) != null) {
            throw new IgniteSQLException("Column CHECK constraints are not supported [colName=" + col.getName() +
                ']', IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
        }

        GridSqlColumn gridCol = new GridSqlColumn(col, null, col.getName());

        gridCol.resultType(GridSqlType.fromColumn(col));

        return gridCol;
    }

    /**
     * Parse {@code ALTER TABLE ... ADD COLUMN} statement.
     * @param addCol H2 statement.
     * @return Grid SQL statement.
     *
     * @see <a href="http://www.h2database.com/html/grammar.html#alter_table_add"></a>
     */
    private GridSqlStatement parseAddColumn(AlterTableAlterColumn addCol) {
        assert addCol.getType() == CommandInterface.ALTER_TABLE_ADD_COLUMN;

        if (ALTER_COLUMN_BEFORE_COL.get(addCol) != null )
            throw new IgniteSQLException("BEFORE keyword is not supported", IgniteQueryErrorCode.UNSUPPORTED_OPERATION);

        if (ALTER_COLUMN_AFTER_COL.get(addCol) != null)
            throw new IgniteSQLException("AFTER keyword is not supported", IgniteQueryErrorCode.UNSUPPORTED_OPERATION);

        if (ALTER_COLUMN_FIRST.get(addCol))
            throw new IgniteSQLException("FIRST keyword is not supported", IgniteQueryErrorCode.UNSUPPORTED_OPERATION);

        GridSqlAlterTableAddColumn res = new GridSqlAlterTableAddColumn();

        ArrayList<Column> h2NewCols = ALTER_COLUMN_NEW_COLS.get(addCol);

        GridSqlColumn[] gridNewCols = new GridSqlColumn[h2NewCols.size()];

        for (int i = 0; i < h2NewCols.size(); i++) {
            Column col = h2NewCols.get(i);

            if (col.getDefaultExpression() != null) {
                throw new IgniteSQLException("ALTER TABLE ADD COLUMN with DEFAULT value is not supported " +
                    "[col=" + col.getName() + ']', IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
            }

            gridNewCols[i] = parseColumn(h2NewCols.get(i));
        }

        res.columns(gridNewCols);

        if (gridNewCols.length == 1)
            res.ifNotExists(ALTER_COLUMN_IF_NOT_EXISTS.get(addCol));

        res.ifTableExists(ALTER_COLUMN_IF_TBL_EXISTS.get(addCol));

        Schema schema = SCHEMA_COMMAND_SCHEMA.get(addCol);

        res.schemaName(schema.getName());

        res.tableName(ALTER_COLUMN_TBL_NAME.get(addCol));

        return res;
    }

    /**
     * Parse {@code ALTER TABLE ... DROP COLUMN} statement.
     * @param dropCol H2 statement.
     * @see <a href="http://www.h2database.com/html/grammar.html#alter_table_add"></a>
     */
    private GridSqlStatement parseDropColumn(AlterTableAlterColumn dropCol) {
        assert dropCol.getType() == CommandInterface.ALTER_TABLE_DROP_COLUMN;

        GridSqlAlterTableDropColumn res = new GridSqlAlterTableDropColumn();

        ArrayList<Column> h2DropCols = ALTER_COLUMN_REMOVE_COLS.get(dropCol);

        String[] gridDropCols = new String[h2DropCols.size()];

        for (int i = 0; i < h2DropCols.size(); i++)
            gridDropCols[i] = h2DropCols.get(i).getName();

        res.columns(gridDropCols);

        if (gridDropCols.length == 1)
            res.ifExists(!ALTER_COLUMN_IF_NOT_EXISTS.get(dropCol));

        res.ifTableExists(ALTER_COLUMN_IF_TBL_EXISTS.get(dropCol));

        Schema schema = SCHEMA_COMMAND_SCHEMA.get(dropCol);

        res.schemaName(schema.getName());

        res.tableName(ALTER_COLUMN_TBL_NAME.get(dropCol));

        return res;
    }

    /**
     * @param name Param name.
     * @param val Param value.
     * @param res Table params to update.
     */
    private static void processExtraParam(String name, String val, GridSqlCreateTable res) {
        assert !F.isEmpty(name);

        switch (name) {
            case PARAM_TEMPLATE:
                ensureNotEmpty(name, val);

                res.templateName(val);

                break;

            case PARAM_BACKUPS:
                ensureNotEmpty(name, val);

                int backups = parseIntParam(PARAM_BACKUPS, val);

                if (backups < 0) {
                    throw new IgniteSQLException("\"" + PARAM_BACKUPS + "\" cannot be negative: " + backups,
                        IgniteQueryErrorCode.PARSING);
                }

                res.backups(backups);

                break;

            case PARAM_PARALLELISM:
                ensureNotEmpty(name, val);

                int qryPar = parseIntParam(PARAM_PARALLELISM, val);

                if (qryPar <= 0)
                    throw new IgniteSQLException("\"" + PARAM_PARALLELISM + "\" must be positive: " +
                        qryPar, IgniteQueryErrorCode.PARSING);

                res.parallelism(qryPar);

                break;

            case PARAM_ATOMICITY:
                ensureNotEmpty(name, val);

                CacheAtomicityMode atomicityMode;

                if (CacheAtomicityMode.TRANSACTIONAL.name().equalsIgnoreCase(val))
                    atomicityMode = CacheAtomicityMode.TRANSACTIONAL;
                else if (CacheAtomicityMode.ATOMIC.name().equalsIgnoreCase(val))
                    atomicityMode = CacheAtomicityMode.ATOMIC;
                else if (CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT.name().equalsIgnoreCase(val))
                    atomicityMode = CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
                else {
                    throw new IgniteSQLException("Invalid value of \"" + PARAM_ATOMICITY + "\" parameter " +
                        "(should be either TRANSACTIONAL or ATOMIC): " + val, IgniteQueryErrorCode.PARSING);
                }

                res.atomicityMode(atomicityMode);

                break;

            case PARAM_CACHE_NAME:
                ensureNotEmpty(name, val);

                res.cacheName(val);

                break;

            case PARAM_KEY_TYPE:
                ensureNotEmpty(name, val);

                res.keyTypeName(val);

                break;

            case PARAM_VAL_TYPE:
                ensureNotEmpty(name, val);

                res.valueTypeName(val);

                break;

            case PARAM_CACHE_GROUP_OLD:
            case PARAM_CACHE_GROUP:
                ensureNotEmpty(name, val);

                res.cacheGroup(val);

                break;

            case PARAM_AFFINITY_KEY_OLD:
            case PARAM_AFFINITY_KEY:
                ensureNotEmpty(name, val);

                String affColName = null;

                // Either strip column name off its quotes, or uppercase it.
                if (val.startsWith("'")) {
                    if (val.length() == 1 || !val.endsWith("'")) {
                        throw new IgniteSQLException("Affinity key column name does not have trailing quote: " + val,
                            IgniteQueryErrorCode.PARSING);
                    }

                    val = val.substring(1, val.length() - 1);

                    ensureNotEmpty(name, val);

                    affColName = val;
                }
                else {
                    for (String colName : res.columns().keySet()) {
                        if (val.equalsIgnoreCase(colName)) {
                            if (affColName != null) {
                                throw new IgniteSQLException("Ambiguous affinity column name, use single quotes " +
                                    "for case sensitivity: " + val, IgniteQueryErrorCode.PARSING);
                            }

                            affColName = colName;
                        }
                    }
                }

                if (affColName == null || !res.columns().containsKey(affColName)) {
                    throw new IgniteSQLException("Affinity key column with given name not found: " + val,
                        IgniteQueryErrorCode.PARSING);
                }

                if (!res.primaryKeyColumns().contains(affColName)) {
                    throw new IgniteSQLException("Affinity key column must be one of key columns: " + affColName,
                        IgniteQueryErrorCode.PARSING);
                }

                res.affinityKey(affColName);

                break;

            case PARAM_WRITE_SYNC:
                ensureNotEmpty(name, val);

                CacheWriteSynchronizationMode writeSyncMode;

                if (CacheWriteSynchronizationMode.FULL_ASYNC.name().equalsIgnoreCase(val))
                    writeSyncMode = CacheWriteSynchronizationMode.FULL_ASYNC;
                else if (CacheWriteSynchronizationMode.FULL_SYNC.name().equalsIgnoreCase(val))
                    writeSyncMode = CacheWriteSynchronizationMode.FULL_SYNC;
                else if (CacheWriteSynchronizationMode.PRIMARY_SYNC.name().equalsIgnoreCase(val))
                    writeSyncMode = CacheWriteSynchronizationMode.PRIMARY_SYNC;
                else {
                    throw new IgniteSQLException("Invalid value of \"" + PARAM_WRITE_SYNC + "\" parameter " +
                        "(should be FULL_SYNC, FULL_ASYNC, or PRIMARY_SYNC): " + val, IgniteQueryErrorCode.PARSING);
                }

                res.writeSynchronizationMode(writeSyncMode);

                break;

            case PARAM_WRAP_KEY: {
                res.wrapKey(F.isEmpty(val) || Boolean.parseBoolean(val));

                break;
            }

            case PARAM_WRAP_VALUE:
                res.wrapValue(F.isEmpty(val) || Boolean.parseBoolean(val));

                break;

            case PARAM_DATA_REGION:
                ensureNotEmpty(name, val);

                res.dataRegionName(val);

                break;

            case PARAM_ENCRYPTED:
                res.encrypted(F.isEmpty(val) || Boolean.parseBoolean(val));

                break;

            default:
                throw new IgniteSQLException("Unsupported parameter: " + name, IgniteQueryErrorCode.PARSING);
        }
    }

    /**
     * Check that param with mandatory value has it specified.
     * @param name Param name.
     * @param val Param value to check.
     */
    private static void ensureNotEmpty(String name, String val) {
        if (F.isEmpty(val))
            throw new IgniteSQLException("Parameter value cannot be empty: " + name, IgniteQueryErrorCode.PARSING);
    }

    /**
     * Parse given value as integer, or throw an {@link IgniteSQLException} if it's not of matching format.
     * @param name param name.
     * @param val param value.
     * @return parsed int value.
     */
    private static int parseIntParam(String name, String val) {
        try {
            return Integer.parseInt(val);
        }
        catch (NumberFormatException ignored) {
            throw new IgniteSQLException("Parameter value must be an integer [name=" + name + ", value=" + val + ']',
                IgniteQueryErrorCode.PARSING);
        }
    }

    /**
     * @param sortOrder Sort order.
     * @param qry Query.
     */
    private void processSortOrder(SortOrder sortOrder, GridSqlQuery qry) {
        if (sortOrder == null)
            return;

        int[] indexes = sortOrder.getQueryColumnIndexes();
        int[] sortTypes = sortOrder.getSortTypes();

        for (int i = 0; i < indexes.length; i++) {
            int colIdx = indexes[i];
            int type = sortTypes[i];

            qry.addSort(new GridSqlSortColumn(colIdx,
                (type & SortOrder.DESCENDING) == 0,
                (type & SortOrder.NULLS_FIRST) != 0,
                (type & SortOrder.NULLS_LAST) != 0));
        }
    }

    /**
     * @param qry Prepared.
     * @return Query.
     */
    public static Query query(Prepared qry) {
        if (qry instanceof Query)
            return (Query)qry;

        if (qry instanceof Explain)
            return query(EXPLAIN_COMMAND.get((Explain)qry));

        throw new CacheException("Unsupported query: " + qry);
    }

    /**
     * Check whether statement is DML statement.
     *
     * @param stmt Statement.
     * @return {@code True} if this is DML.
     */
    public static boolean isDml(Prepared stmt) {
        return stmt instanceof Merge || stmt instanceof Insert || stmt instanceof Update || stmt instanceof Delete;
    }

    /**
     * @param stmt Prepared.
     * @return Target table.
     */
    @NotNull public static GridH2Table dmlTable(@NotNull Prepared stmt) {
        Table table;

        if (stmt.getClass() == Insert.class)
            table = INSERT_TABLE.get((Insert)stmt);
        else if (stmt.getClass() == Merge.class)
            table = MERGE_TABLE.get((Merge)stmt);
        else if (stmt.getClass() == Delete.class)
            table = DELETE_FROM.get((Delete)stmt).getTable();
        else if (stmt.getClass() == Update.class)
            table = UPDATE_TARGET.get((Update)stmt).getTable();
        else
            throw new IgniteException("Unsupported statement: " + stmt);

        assert table instanceof GridH2Table : table;

        return (GridH2Table) table;
    }

    /**
     * Check if query may be run locally on all caches mentioned in the query.
     *
     * @return {@code true} if query may be run locally on all caches mentioned in the query, i.e. there's no need
     *     to run distributed query.
     */
    public boolean isLocalQuery() {
        if (selectForUpdate)
            return false;

        for (Object o : h2ObjToGridObj.values()) {
            if (o instanceof GridSqlAlias)
                o = GridSqlAlias.unwrap((GridSqlAst)o);

            if (o instanceof GridSqlTable) {
                GridH2Table tbl = ((GridSqlTable)o).dataTable();

                if (tbl != null) {
                    GridCacheContext<?, ?> cctx = tbl.cacheContext();

                    //It's not affinity cache. Can't be local.
                    if (cctx == null)
                        return false;

                    if (cctx.mvccEnabled())
                        return false;

                    if (cctx.isPartitioned())
                        return false;

                    if (isReplicatedLocalExecutionImpossible(cctx))
                        return false;
                }
            }
        }

        return true;
    }

    /** */
    private static boolean isReplicatedLocalExecutionImpossible(GridCacheContext<?, ?> cctx) {
        // Improvement is possible:
        // MOVING partitions check inspects full partition map, but possibly only local node check is sufficient.
        return cctx.isReplicated() && (!cctx.affinityNode() || cctx.topology().hasMovingPartitions());
    }

    /**
     * Get first (i.e. random, as we need any one) partitioned cache from parsed query
     *     to determine expected query parallelism.
     * @return Context for the first of partitioned caches mentioned in the query,
     *     or {@code null} if it does not involve partitioned caches.
     */
    public GridCacheContext getFirstPartitionedCache() {
        for (Object o : h2ObjToGridObj.values()) {
            if (o instanceof GridSqlAlias)
                o = GridSqlAlias.unwrap((GridSqlAst)o);

            if (o instanceof GridSqlTable) {
                GridH2Table tbl = ((GridSqlTable)o).dataTable();

                if (tbl != null && tbl.cacheContext().isPartitioned())
                    return tbl.cacheContext();
            }
        }

        return null;
    }

    /**
     * @return All known cache IDs.
     */
    public List<Integer> cacheIds() {
        ArrayList<Integer> res = new ArrayList<>(1);

        for (Object o : h2ObjToGridObj.values()) {
            if (o instanceof GridSqlAlias)
                o = GridSqlAlias.unwrap((GridSqlAst)o);

            if (o instanceof GridSqlTable) {
                GridH2Table tbl = ((GridSqlTable)o).dataTable();

                if (tbl != null)
                    res.add(tbl.cacheId());
            }
        }

        return res;
    }

    /**
     * Extract all tables participating in DML statement.
     *
     * @return List of tables participate at query.
     * @throws IgniteSQLException in case query contains virtual tables.
     */
    public List<GridH2Table> tablesForDml() throws IgniteSQLException {
        Collection<?> parserObjects = h2ObjToGridObj.values();

        List<GridH2Table> tbls = new ArrayList<>(parserObjects.size());

        // check all involved caches
        for (Object o : parserObjects) {
            if (o instanceof GridSqlMerge)
                o = ((GridSqlMerge) o).into();
            else if (o instanceof GridSqlInsert)
                o = ((GridSqlInsert) o).into();
            else if (o instanceof GridSqlUpdate)
                o = ((GridSqlUpdate) o).target();
            else if (o instanceof GridSqlDelete)
                o = ((GridSqlDelete) o).from();

            if (o instanceof GridSqlAlias)
                o = GridSqlAlias.unwrap((GridSqlAst)o);

            if (o instanceof GridSqlTable) {
                GridH2Table h2tbl = ((GridSqlTable)o).dataTable();

                if (h2tbl == null) { // Check for virtual tables.
                    throw new IgniteSQLException("Operation not supported for table '" +
                        ((GridSqlTable)o).tableName() + "'", IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
                }

                tbls.add(h2tbl);
            }
        }

        return tbls;
    }

    /**
     * Parse query.
     *
     * @param prepared Prepared statement.
     * @param useOptimizedSubqry Whether to user optimized subquery.
     * @return Parsed query.
     */
    public static GridSqlQuery parseQuery(Prepared prepared, boolean useOptimizedSubqry, IgniteLogger log) {
        return (GridSqlQuery)new GridSqlQueryParser(useOptimizedSubqry, log).parse(prepared);
    }

    /**
     * @param stmt Prepared statement.
     * @return Parsed AST.
     */
    public final GridSqlStatement parse(Prepared stmt) {
        if (stmt instanceof Query) {
            if (optimizedTableFilterOrder != null)
                collectOptimizedTableFiltersOrder((Query)stmt);

            selectForUpdate = isForUpdateQuery(stmt);

            return parseQuery((Query)stmt);
        }

        if (stmt instanceof Merge)
            return parseMerge((Merge)stmt);

        if (stmt instanceof Insert)
            return parseInsert((Insert)stmt);

        if (stmt instanceof Delete)
            return parseDelete((Delete)stmt);

        if (stmt instanceof Update)
            return parseUpdate((Update)stmt);

        if (stmt instanceof Explain)
            return parse(EXPLAIN_COMMAND.get((Explain)stmt)).explain(true);

        if (stmt instanceof CreateIndex)
            return parseCreateIndex((CreateIndex)stmt);

        if (stmt instanceof DropIndex)
            return parseDropIndex((DropIndex)stmt);

        if (stmt instanceof CreateTable)
            return parseCreateTable((CreateTable)stmt);

        if (stmt instanceof DropTable)
            return parseDropTable((DropTable)stmt);

        if (stmt instanceof AlterTableAlterColumn)
            return parseAlterColumn((AlterTableAlterColumn)stmt);

        throw new IgniteSQLException("Unsupported statement: " + stmt,
            IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
    }

    /**
     * @return H2 to Grid objects map.
     */
    public Map<Object, Object> objectsMap() {
        return h2ObjToGridObj;
    }

    /**
     * @param qry Query.
     * @return Parsed query AST.
     */
    private GridSqlQuery parseQuery(Query qry) {
        if (qry instanceof Select)
            return parseSelect((Select)qry);

        if (qry instanceof SelectUnion)
            return parseUnion((SelectUnion)qry);

        throw new UnsupportedOperationException("Unknown query type: " + qry);
    }

    /**
     * @param union Select.
     * @return Parsed AST.
     */
    private GridSqlUnion parseUnion(SelectUnion union) {
        if (UNION_IS_FOR_UPDATE.get(union))
            throw new IgniteSQLException("SELECT UNION FOR UPDATE is not supported.",
                IgniteQueryErrorCode.UNSUPPORTED_OPERATION);

        GridSqlUnion res = (GridSqlUnion)h2ObjToGridObj.get(union);

        if (res != null)
            return res;

        res = new GridSqlUnion();

        res.right(parseQuery(union.getRight()));
        res.left(parseQuery(union.getLeft()));

        res.unionType(union.getUnionType());

        res.limit(parseExpression(union.getLimit(), false));
        res.offset(parseExpression(union.getOffset(), false));

        processSortOrder(UNION_SORT.get(union), res);

        h2ObjToGridObj.put(union, res);

        return res;
    }

    /**
     * @param expression Expression.
     * @param calcTypes Calculate types for all the expressions.
     * @return Parsed expression.
     */
    private GridSqlElement parseExpression(@Nullable Expression expression, boolean calcTypes) {
        if (expression == null)
            return null;

        GridSqlElement res = (GridSqlElement)h2ObjToGridObj.get(expression);

        if (res == null) {
            res = parseExpression0(expression, calcTypes);

            if (calcTypes)
                res.resultType(fromExpression(expression));

            h2ObjToGridObj.put(expression, res);
        }

        return res;
    }

    /**
     * @param qry Query.
     */
    private void collectOptimizedTableFiltersOrder(Query qry) {
        if (qry instanceof SelectUnion) {
            collectOptimizedTableFiltersOrder(((SelectUnion)qry).getLeft());
            collectOptimizedTableFiltersOrder(((SelectUnion)qry).getRight());
        }
        else {
            Select select = (Select)qry;

            TableFilter filter = select.getTopTableFilter();

            int i = 0;

            do {
                assert0(filter != null, select);
                assert0(filter.getNestedJoin() == null, select);

                // Here all the table filters must have generated unique aliases,
                // thus we can store them in the same map for all the subqueries.
                optimizedTableFilterOrder.put(filter.getTableAlias(), i++);

                Table tbl = filter.getTable();

                // Go down and collect inside of optimized subqueries.
                if (tbl instanceof TableView) {
                    ViewIndex viewIdx = (ViewIndex)filter.getIndex();

                    collectOptimizedTableFiltersOrder(viewIdx.getQuery());
                }

                filter = filter.getJoin();
            }
            while (filter != null);
        }
    }

    /**
     * Map operation type.
     *
     * @param opType H2 operation type.
     * @return Ignite operation type.
     */
    private static GridSqlOperationType mapOperationType(Operation.OpType opType) {
        switch (opType) {
            case CONCAT:
                return GridSqlOperationType.CONCAT;

            case PLUS:
                return GridSqlOperationType.PLUS;

            case MINUS:
                return GridSqlOperationType.MINUS;

            case MULTIPLY:
                return GridSqlOperationType.MULTIPLY;

            case DIVIDE:
                return GridSqlOperationType.DIVIDE;

            case NEGATE:
                // NB: Was set to null in original code for some reason; left unchanged during 1.4.197 migration.
                return null;

            case MODULUS:
                return GridSqlOperationType.MODULUS;

            default:
                throw new IllegalStateException("Unsupported operation type: " + opType);
        }
    }

    /**
     * @param expression Expression.
     * @param calcTypes Calculate types for all the expressions.
     * @return Parsed expression.
     */
    private GridSqlElement parseExpression0(Expression expression, boolean calcTypes) {
        if (expression instanceof ExpressionColumn) {
            ExpressionColumn expCol = (ExpressionColumn)expression;

            return new GridSqlColumn(expCol.getColumn(),
                parseTableFilter(expCol.getTableFilter()),
                SCHEMA_NAME.get(expCol),
                expCol.getOriginalTableAliasName(),
                expCol.getColumnName());
        }

        if (expression instanceof Alias)
            return new GridSqlAlias(expression.getAlias(),
                parseExpression(expression.getNonAliasExpression(), calcTypes), true);

        if (expression instanceof ValueExpression)
            // == comparison is legit, see ValueExpression#getSQL()
            return expression == ValueExpression.getDefault() ? GridSqlKeyword.DEFAULT :
                new GridSqlConst(expression.getValue(null));

        if (expression instanceof Operation) {
            Operation operation = (Operation)expression;

            Operation.OpType type = OPERATION_TYPE.get(operation);

            if (type == Operation.OpType.NEGATE) {
                assert OPERATION_RIGHT.get(operation) == null;

                return new GridSqlOperation(GridSqlOperationType.NEGATE,
                    parseExpression(OPERATION_LEFT.get(operation), calcTypes));
            }

            return new GridSqlOperation(mapOperationType(type),
                parseExpression(OPERATION_LEFT.get(operation), calcTypes),
                parseExpression(OPERATION_RIGHT.get(operation), calcTypes));
        }

        if (expression instanceof Comparison) {
            Comparison cmp = (Comparison)expression;

            GridSqlOperationType opType = COMPARISON_TYPES[COMPARISON_TYPE.get(cmp)];

            assert opType != null : COMPARISON_TYPE.get(cmp);

            Expression leftExp = COMPARISON_LEFT.get(cmp);
            GridSqlElement left = parseExpression(leftExp, calcTypes);

            if (opType.childrenCount() == 1)
                return new GridSqlOperation(opType, left);

            Expression rightExp = COMPARISON_RIGHT.get(cmp);
            GridSqlElement right = parseExpression(rightExp, calcTypes);

            return new GridSqlOperation(opType, left, right);
        }

        if (expression instanceof ConditionNot)
            return new GridSqlOperation(NOT, parseExpression(expression.getNotIfPossible(null), calcTypes));

        if (expression instanceof ConditionAndOr) {
            ConditionAndOr andOr = (ConditionAndOr)expression;

            int type = ANDOR_TYPE.get(andOr);

            assert type == ConditionAndOr.AND || type == ConditionAndOr.OR;

            return new GridSqlOperation(type == ConditionAndOr.AND ? AND : OR,
                parseExpression(ANDOR_LEFT.get(andOr), calcTypes), parseExpression(ANDOR_RIGHT.get(andOr), calcTypes));
        }

        if (expression instanceof Subquery) {
            Query qry = ((Subquery)expression).getQuery();

            return parseQueryExpression(qry);
        }

        if (expression instanceof ConditionIn) {
            GridSqlOperation res = new GridSqlOperation(IN);

            res.addChild(parseExpression(LEFT_CI.get((ConditionIn)expression), calcTypes));

            List<Expression> vals = VALUE_LIST_CI.get((ConditionIn)expression);

            for (Expression val : vals)
                res.addChild(parseExpression(val, calcTypes));

            return res;
        }

        if (expression instanceof ConditionInConstantSet) {
            GridSqlOperation res = new GridSqlOperation(IN);

            res.addChild(parseExpression(LEFT_CICS.get((ConditionInConstantSet)expression), calcTypes));

            List<Expression> vals = VALUE_LIST_CICS.get((ConditionInConstantSet)expression);

            for (Expression val : vals)
                res.addChild(parseExpression(val, calcTypes));

            return res;
        }

        if (expression instanceof ConditionInSelect) {
            GridSqlOperation res = new GridSqlOperation(IN);

            boolean all = ALL.get((ConditionInSelect)expression);
            int compareType = COMPARE_TYPE.get((ConditionInSelect)expression);

            assert0(!all, expression);
            assert0(compareType == Comparison.EQUAL, expression);

            res.addChild(parseExpression(LEFT_CIS.get((ConditionInSelect)expression), calcTypes));

            Query qry = QUERY_IN.get((ConditionInSelect)expression);

            res.addChild(parseQueryExpression(qry));

            return res;
        }

        if (expression instanceof CompareLike) {
            assert0(ESCAPE.get((CompareLike)expression) == null, expression);

            boolean regexp = REGEXP_CL.get((CompareLike)expression);

            return new GridSqlOperation(regexp ? REGEXP : LIKE,
                parseExpression(LEFT.get((CompareLike)expression), calcTypes),
                parseExpression(RIGHT.get((CompareLike)expression), calcTypes));
        }

        if (expression instanceof Function) {
            Function f = (Function)expression;

            GridSqlFunction res = new GridSqlFunction(null, f.getName());

            if (f.getArgs() != null) {
                if (f.getFunctionType() == Function.TABLE || f.getFunctionType() == Function.TABLE_DISTINCT) {
                    Column[] cols = FUNC_TBL_COLS.get((TableFunction)f);
                    Expression[] args = f.getArgs();

                    assert cols.length == args.length;

                    for (int i = 0; i < cols.length; i++) {
                        GridSqlElement arg = parseExpression(args[i], calcTypes);

                        GridSqlAlias alias = new GridSqlAlias(cols[i].getName(), arg, false);

                        alias.resultType(fromColumn(cols[i]));

                        res.addChild(alias);
                    }
                }
                else {
                    for (Expression arg : f.getArgs()) {
                        if (arg == null) {
                            if (f.getFunctionType() != Function.CASE)
                                throw new IllegalStateException("Function type with null arg: " + f.getFunctionType());

                            res.addChild(GridSqlPlaceholder.EMPTY);
                        }
                        else
                            res.addChild(parseExpression(arg, calcTypes));
                    }
                }
            }

            if (f.getFunctionType() == Function.CAST || f.getFunctionType() == Function.CONVERT) {
                checkTypeSupported(f.getType(), "[expSql=" + f.getSQL() + ']');

                res.resultType(fromExpression(f));
            }

            return res;
        }

        if (expression instanceof JavaFunction) {
            JavaFunction f = (JavaFunction)expression;

            FunctionAlias alias = FUNC_ALIAS.get(f);

            GridSqlFunction res = new GridSqlFunction(alias.getSchema().getName(), f.getName());

            if (f.getArgs() != null) {
                for (Expression arg : f.getArgs())
                    res.addChild(parseExpression(arg, calcTypes));
            }

            return res;
        }

        if (expression instanceof Parameter)
            return new GridSqlParameter(((Parameter)expression).getIndex());

        if (expression instanceof Aggregate) {
            Aggregate.AggregateType type = TYPE.get((Aggregate)expression);

            if (GridSqlAggregateFunction.isValidType(type)) {
                GridSqlAggregateFunction res = new GridSqlAggregateFunction(
                    DISTINCT.get((Aggregate)expression), type);

                Expression on = ON.get((Aggregate)expression);

                if (on != null)
                    res.addChild(parseExpression(on, calcTypes));

                ArrayList<SelectOrderBy> orders = GROUP_CONCAT_ORDER_LIST.get((Aggregate)expression);

                if (!F.isEmpty(orders))
                    parseGroupConcatOrder(res, orders, calcTypes);

                Expression separator = GROUP_CONCAT_SEPARATOR.get((Aggregate)expression);

                if (separator!= null)
                    res.setGroupConcatSeparator(parseExpression(separator, calcTypes));

                return res;
            }
        }

        if (expression instanceof ExpressionList) {
            Expression[] exprs = EXPR_LIST.get((ExpressionList)expression);

            GridSqlArray res = new GridSqlArray(exprs.length);

            for (Expression expr : exprs)
                res.addChild(parseExpression(expr, calcTypes));

            return res;
        }

        if (expression instanceof ConditionExists) {
            Query qry = QUERY_EXISTS.get((ConditionExists)expression);

            GridSqlOperation res = new GridSqlOperation(EXISTS);

            res.addChild(parseQueryExpression(qry));

            return res;
        }

        throw new IgniteException("Unsupported expression: " + expression + " [type=" +
            expression.getClass().getSimpleName() + ']');
    }

    /**
     * Check if passed statement is insert statement eligible for streaming.
     *
     * @param prep Prepared statement.
     * @return {@code True} if streamable insert.
     */
    public static boolean isStreamableInsertStatement(Prepared prep) {
        return prep instanceof Insert && INSERT_QUERY.get((Insert)prep) == null;
    }

    /**
     * @param f Aggregate function.
     * @param orders Orders.
     * @param calcTypes Calculate types for all the expressions.
     */
    private void parseGroupConcatOrder(GridSqlAggregateFunction f, ArrayList<SelectOrderBy> orders,
        boolean calcTypes) {
        GridSqlElement[] grpConcatOrderExpression = new GridSqlElement[orders.size()];
        boolean[] grpConcatOrderDesc = new boolean[orders.size()];

        for (int i = 0; i < orders.size(); ++i) {
            SelectOrderBy o = orders.get(i);

            grpConcatOrderExpression[i] = parseExpression(o.expression, calcTypes);
            grpConcatOrderDesc[i] = o.descending;
        }

        f.setGroupConcatOrder(grpConcatOrderExpression, grpConcatOrderDesc);
    }

    /**
     * @param cond Condition.
     * @param o Object.
     */
    private static void assert0(boolean cond, Object o) {
        if (!cond)
            throw new IgniteException("Unsupported query: " + o);
    }

    /**
     * Determines if specified prepared statement is an EXPLAIN of update operation: UPDATE, DELETE, etc.
     * (e.g. not a SELECT query).
     *
     * @param statement statement to probe.
     * @return {@code True} if statement is EXPLAIN UPDATE, EXPLAIN DELETE or etc.; {@code false} otherwise.
     */
    public static boolean isExplainUpdate(Prepared statement) {
        if (!(statement instanceof Explain))
            return false;

        return !EXPLAIN_COMMAND.get((Explain)statement).isQuery();
    }

    /**
     */
    public static void checkTypeSupported(int type, String errMsg) {
        if (type == Value.TIMESTAMP_TZ) {
            throw new IgniteSQLException("TIMESTAMP WITH TIMEZONE type is not supported " + errMsg,
                IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
        }

        if (type == Value.ENUM) {
            throw new IgniteSQLException("ENUM type is not supported " + errMsg,
                IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
        }
    }

    /**
     * @param cls Class.
     * @param fldName Fld name.
     */
    private static <T, R> Getter<T, R> getter(Class<? extends T> cls, String fldName) {
        Field field;

        try {
            field = cls.getDeclaredField(fldName);
        }
        catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }

        field.setAccessible(true);

        return new Getter<>(field);
    }

    /**
     * Field getter.
     */
    public static class Getter<T, R> {
        /** */
        private final Field fld;

        /**
         * @param fld Fld.
         */
        private Getter(Field fld) {
            this.fld = fld;
        }

        /**
         * @param obj Object.
         * @return Result.
         */
        public R get(T obj) {
            try {
                return (R)fld.get(obj);
            }
            catch (IllegalAccessException e) {
                throw new IgniteException(e);
            }
        }
    }

    /**
     *
     */
    public static class PreparedWithRemaining {
        /** Prepared. */
        private Prepared prepared;

        /** Remaining sql. */
        private String remainingSql;

        /**
         * @param prepared Prepared.
         * @param sql Remaining SQL.
         */
        public PreparedWithRemaining(Prepared prepared, String sql) {
            this.prepared = prepared;

            if (sql != null)
                sql = sql.trim();

            remainingSql = !F.isEmpty(sql) ? sql : null;
        }

        /**
         * @return Prepared.
         */
        public Prepared prepared() {
            return prepared;
        }

        /**
         * @return Remaining SQL.
         */
        public String remainingSql() {
            return remainingSql;
        }
    }
}
