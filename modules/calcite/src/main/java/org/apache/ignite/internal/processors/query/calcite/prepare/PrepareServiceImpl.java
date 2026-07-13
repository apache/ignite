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

package org.apache.ignite.internal.processors.query.calcite.prepare;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.ValidationException;
import org.apache.ignite.cache.query.QueryCancelledException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.calcite.DistributedCalciteConfiguration;
import org.apache.ignite.internal.processors.query.calcite.prepare.SelectForUpdatePlan.LockTarget;
import org.apache.ignite.internal.processors.query.calcite.prepare.ddl.DdlSqlToCommandConverter;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.sql.IgniteSqlSelectForUpdate;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.AbstractService;
import org.apache.ignite.internal.processors.query.calcite.util.TypeUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.jetbrains.annotations.Nullable;

import static java.util.Collections.singletonList;
import static org.apache.calcite.rel.type.RelDataType.PRECISION_NOT_SPECIFIED;
import static org.apache.ignite.internal.processors.query.calcite.prepare.PlannerHelper.optimize;

/**
 *
 */
@SuppressWarnings("TypeMayBeWeakened")
public class PrepareServiceImpl extends AbstractService implements PrepareService {
    /** */
    private final DdlSqlToCommandConverter ddlConverter;

    /** */
    private final PlanExtractor planExtractor;

    /** */
    private DistributedCalciteConfiguration distrCfg;

    /**
     * @param ctx Kernal.
     */
    public PrepareServiceImpl(GridKernalContext ctx) {
        super(ctx);

        planExtractor = new PlanExtractor(ctx);
        ddlConverter = new DdlSqlToCommandConverter();
    }

    /** {@inheritDoc} */
    @Override public void onStart(GridKernalContext ctx) {
        super.onStart(ctx);

        distrCfg = queryProcessor(ctx).distributedConfiguration();
    }

    /** {@inheritDoc} */
    @Override public QueryPlan prepareSingle(SqlNode sqlNode, PlanningContext ctx) {
        try {
            assert single(sqlNode);

            ctx.planner().reset();

            assert distrCfg != null;

            String[] disbledRules = distrCfg.disabledRules();

            if (!F.isEmpty(disbledRules))
                ctx.addRulesFilter(new IgnitePlanner.DisabledRuleFilter(disbledRules));

            if (sqlNode instanceof IgniteSqlSelectForUpdate)
                return prepareSelectForUpdate((IgniteSqlSelectForUpdate)sqlNode, ctx);

            if (SqlKind.DDL.contains(sqlNode.getKind()))
                return prepareDdl(sqlNode, ctx);

            switch (sqlNode.getKind()) {
                case SELECT:
                case ORDER_BY:
                case WITH:
                case VALUES:
                case UNION:
                case EXCEPT:
                case INTERSECT:
                    return prepareQuery(sqlNode, ctx);

                case INSERT:
                case DELETE:
                case UPDATE:
                case MERGE:
                    return prepareDml(sqlNode, ctx);

                case EXPLAIN:
                    return prepareExplain(sqlNode, ctx);

                default:
                    throw new IgniteSQLException("Unsupported operation [" +
                        "sqlNodeKind=" + sqlNode.getKind() + "; " +
                        "querySql=\"" + ctx.query() + "\"]", IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
            }
        }
        catch (ValidationException | CalciteContextException e) {
            throw new IgniteSQLException("Failed to validate query. " + e.getMessage(), IgniteQueryErrorCode.PARSING, e);
        }
        catch (RelOptPlanner.CannotPlanException e) {
            // In most cases this exception is thrown if there is not enough time to produce any working plan
            // (due to planning timeout).
            IgniteSQLException ex = new IgniteSQLException("Failed to plan query",
                IgniteQueryErrorCode.QUERY_CANCELED, new QueryCancelledException());

            ex.addSuppressed(e);

            throw ex;
        }
    }

    /**
     * Prepares a {@code SELECT ... FOR UPDATE} statement.
     *
     * <p>Steps:
     * <ol>
     *   <li>Validate that the inner query is a plain {@link SqlSelect} (not UNION etc.).</li>
     *   <li>Collect base tables from the FROM clause.</li>
     *   <li>Append OF columns for validation and lock columns for every table.</li>
     *   <li>Prepare the modified SELECT as a normal {@link MultiStepQueryPlan}.</li>
     *   <li>Resolve tables selected by OF using aliases and validated column origins.</li>
     *   <li>Return a {@link SelectForUpdatePlan} wrapping the inner plan.</li>
     * </ol>
     */
    private SelectForUpdatePlan prepareSelectForUpdate(IgniteSqlSelectForUpdate forUpdate, PlanningContext ctx)
        throws ValidationException {
        SqlNode innerQuery = forUpdate.query();

        if (!(innerQuery instanceof SqlSelect))
            throw new IgniteSQLException(
                "SELECT FOR UPDATE is only supported for plain SELECT statements",
                IgniteQueryErrorCode.UNSUPPORTED_OPERATION);

        SqlSelect select = (SqlSelect)innerQuery;

        validateSelectForUpdateShape(select, ctx.planner());

        // Unwrap optional AS alias around the table reference.
        SqlNode from = select.getFrom();

        if (from == null)
            throw new IgniteSQLException(
                "SELECT FOR UPDATE requires a FROM clause",
                IgniteQueryErrorCode.UNSUPPORTED_OPERATION);

        List<TableRef> tableRefs = new ArrayList<>();

        collectTableRefs(from, ctx.schemaName(), tableRefs);

        SqlNodeList origList = select.getSelectList();
        SqlNodeList newList = new SqlNodeList(SqlParserPos.ZERO);

        for (SqlNode col : origList)
            newList.add(col);

        SqlNodeList ofList = forUpdate.ofList();
        int ofColumnCount = ofList == null ? 0 : ofList.size();

        if (ofList != null) {
            for (SqlNode col : ofList)
                newList.add(col);
        }

        for (TableRef tableRef : tableRefs) {
            newList.add(tableRef.column(QueryUtils.KEY_FIELD_NAME));
            newList.add(tableRef.column(QueryUtils.VAL_FIELD_NAME));
            newList.add(tableRef.column(QueryUtils.VER_FIELD_NAME));
        }

        SqlSelect modifiedSelect = new SqlSelect(
            SqlParserPos.ZERO,
            null,
            newList,
            select.getFrom(),
            select.getWhere(),
            select.getGroup(),
            select.getHaving(),
            select.getWindowList(),
            select.getQualify(),
            select.getOrderList(),
            select.getOffset(),
            select.getFetch(),
            select.getHints()
        );

        MultiStepQueryPlan innerPlan = (MultiStepQueryPlan)prepareQuery(modifiedSelect, ctx);

        int lockColumnCount = tableRefs.size() * 3;
        int userColCount = innerPlan.fieldsMetadata().rowType().getFieldCount() - ofColumnCount - lockColumnCount;
        Set<TableRef> tablesToLock = new LinkedHashSet<>();

        if (ofList == null)
            tablesToLock.addAll(tableRefs);
        else {
            List<GridQueryFieldMetadata> fieldsMeta = innerPlan.fieldsMetadata().queryFieldsMetadata(ctx.typeFactory());

            for (int i = 0; i < ofList.size(); i++) {
                SqlNode ofColumn = ofList.get(i);

                if (!(ofColumn instanceof SqlIdentifier))
                    throw new IgniteSQLException(
                        "SELECT FOR UPDATE OF accepts only column references",
                        IgniteQueryErrorCode.UNSUPPORTED_OPERATION);

                tablesToLock.add(resolveLockTarget(
                    tableRefs,
                    (SqlIdentifier)ofColumn,
                    fieldsMeta.get(userColCount + i)
                ));
            }
        }

        List<LockTarget> lockTargets = new ArrayList<>(tablesToLock.size());

        for (int i = 0; i < tableRefs.size(); i++) {
            TableRef tableRef = tableRefs.get(i);

            if (tablesToLock.contains(tableRef)) {
                lockTargets.add(new LockTarget(
                    tableRef.schemaName,
                    tableRef.tableName,
                    userColCount + ofColumnCount + i * 3
                ));
            }
        }

        return new SelectForUpdatePlan(innerPlan, userColCount, forUpdate.waitSeconds(), lockTargets);
    }

    /** Rejects SELECT forms whose result rows cannot be mapped one-to-one to cache entries. */
    private void validateSelectForUpdateShape(SqlSelect select, IgnitePlanner planner) {
        if (select.isDistinct())
            throw unsupportedSelectForUpdateClause("DISTINCT");

        if (select.getGroup() != null)
            throw unsupportedSelectForUpdateClause("GROUP BY");

        if (select.getHaving() != null)
            throw unsupportedSelectForUpdateClause("HAVING");

        if (planner.isAggregate(select))
            throw unsupportedSelectForUpdateClause("aggregate functions");
    }

    /** */
    private IgniteSQLException unsupportedSelectForUpdateClause(String clause) {
        return new IgniteSQLException(
            "SELECT FOR UPDATE does not support " + clause,
            IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
    }

    /** Collects simple table references from a possibly nested JOIN tree. */
    private void collectTableRefs(SqlNode from, String dfltSchemaName, List<TableRef> tableRefs) {
        if (from instanceof SqlJoin) {
            SqlJoin join = (SqlJoin)from;

            collectTableRefs(join.getLeft(), dfltSchemaName, tableRefs);
            collectTableRefs(join.getRight(), dfltSchemaName, tableRefs);

            return;
        }

        SqlNode tableNode = from;
        SqlIdentifier qualifier = null;

        if (from.getKind() == SqlKind.AS) {
            SqlCall asCall = (SqlCall)from;
            SqlNode aliasNode = asCall.operand(1);

            tableNode = asCall.operand(0);

            if (!(aliasNode instanceof SqlIdentifier))
                throw unsupportedFrom(from);

            qualifier = (SqlIdentifier)aliasNode;
        }

        if (!(tableNode instanceof SqlIdentifier))
            throw unsupportedFrom(from);

        SqlIdentifier tableId = (SqlIdentifier)tableNode;
        int nameCount = tableId.names.size();
        String tableName = tableId.names.get(nameCount - 1);
        String schemaName = nameCount >= 2 ? tableId.names.get(nameCount - 2) : dfltSchemaName;

        if (qualifier == null)
            qualifier = tableId.getComponent(nameCount - 1);

        tableRefs.add(new TableRef(schemaName, tableName, qualifier));
    }

    /** Resolves an OF column to one table reference. */
    private TableRef resolveLockTarget(
        List<TableRef> tableRefs,
        SqlIdentifier column,
        GridQueryFieldMetadata fieldMeta
    ) {
        if (column.names.size() > 1) {
            List<String> qualifier = column.names.subList(0, column.names.size() - 1);
            TableRef match = null;

            for (TableRef tableRef : tableRefs) {
                if (tableRef.matchesQualifier(qualifier)) {
                    if (match != null)
                        throw ambiguousOfColumn(column);

                    match = tableRef;
                }
            }

            if (match != null)
                return match;
        }

        TableRef match = null;

        for (TableRef tableRef : tableRefs) {
            if (tableRef.matchesQualifier(List.of(fieldMeta.schemaName(), fieldMeta.typeName()))) {
                if (match != null)
                    throw ambiguousOfColumn(column);

                match = tableRef;
            }
        }

        if (match == null)
            throw new IgniteSQLException(
                "SELECT FOR UPDATE OF column does not belong to a table in FROM: " + column,
                IgniteQueryErrorCode.UNSUPPORTED_OPERATION);

        return match;
    }

    /** */
    private IgniteSQLException ambiguousOfColumn(SqlIdentifier column) {
        return new IgniteSQLException(
            "SELECT FOR UPDATE OF column is ambiguous: " + column,
            IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
    }

    /** */
    private IgniteSQLException unsupportedFrom(SqlNode from) {
        return new IgniteSQLException(
            "SELECT FOR UPDATE is only supported for tables and JOINs of tables: " + from,
            IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
    }

    /** A base table and the qualifier used to reference it in the SELECT scope. */
    private static class TableRef {
        /** */
        private final String schemaName;

        /** */
        private final String tableName;

        /** */
        private final SqlIdentifier qualifier;

        /** */
        private TableRef(String schemaName, String tableName, SqlIdentifier qualifier) {
            this.schemaName = schemaName;
            this.tableName = tableName;
            this.qualifier = qualifier;
        }

        /** */
        private SqlIdentifier column(String columnName) {
            return qualifier.plus(columnName, SqlParserPos.ZERO);
        }

        /** */
        private boolean matchesQualifier(List<String> names) {
            if (names.size() == 1)
                return qualifier.getSimple().equals(names.get(0));

            int size = names.size();

            return schemaName.equals(names.get(size - 2)) && tableName.equals(names.get(size - 1));
        }
    }

    /**
     *
     */
    private QueryPlan prepareDdl(SqlNode sqlNode, PlanningContext ctx) {
        assert sqlNode instanceof SqlDdl : sqlNode == null ? "null" : sqlNode.getClass().getName();

        return new DdlPlan(ctx.query(), ddlConverter.convert((SqlDdl)sqlNode, ctx));
    }

    /**
     *
     */
    private QueryPlan prepareExplain(SqlNode explain, PlanningContext ctx) throws ValidationException {
        IgnitePlanner planner = ctx.planner();

        SqlNode sql = ((SqlExplain)explain).getExplicandum();

        // Validate
        sql = planner.validate(sql);

        // Convert to Relational operators graph
        IgniteRel igniteRel = optimize(sql, planner, log);

        String plan = RelOptUtil.toString(igniteRel, SqlExplainLevel.ALL_ATTRIBUTES);

        return new ExplainPlan(ctx.query(), plan, explainFieldsMetadata(ctx));
    }

    /** */
    private boolean single(SqlNode sqlNode) {
        return !(sqlNode instanceof SqlNodeList);
    }

    /** */
    private QueryPlan prepareQuery(SqlNode sqlNode, PlanningContext ctx) {
        IgnitePlanner planner = ctx.planner();

        // Validate
        ValidationResult validated = planner.validateAndGetTypeMetadata(sqlNode);

        sqlNode = validated.sqlNode();

        IgniteRel igniteRel = optimize(sqlNode, planner, log);

        String plan = planExtractor.extract(igniteRel);

        // Extract parameters meta.
        FieldsMetadata params = DynamicParamTypeExtractor.go(igniteRel);

        // Split query plan to query fragments.
        List<Fragment> fragments = new Splitter().go(igniteRel);

        QueryTemplate template = new QueryTemplate(fragments);

        return new MultiStepQueryPlan(ctx.query(), plan, template, queryFieldsMetadata(ctx, validated.dataType(),
                validated.origins(), validated.aliases()), params);
    }

    /** */
    private QueryPlan prepareDml(SqlNode sqlNode, PlanningContext ctx) throws ValidationException {
        IgnitePlanner planner = ctx.planner();

        // Validate
        sqlNode = planner.validate(sqlNode);

        // Convert to Relational operators graph
        IgniteRel igniteRel = optimize(sqlNode, planner, log);

        String plan = planExtractor.extract(igniteRel);

        // Extract parameters meta.
        FieldsMetadata params = DynamicParamTypeExtractor.go(igniteRel);

        // Split query plan to query fragments.
        List<Fragment> fragments = new Splitter().go(igniteRel);

        QueryTemplate template = new QueryTemplate(fragments);

        return new MultiStepDmlPlan(ctx.query(), plan, template, queryFieldsMetadata(ctx, igniteRel.getRowType(),
                null, null), params);
    }

    /** */
    private FieldsMetadata queryFieldsMetadata(
        PlanningContext ctx,
        RelDataType sqlType,
        @Nullable List<List<String>> origins,
        @Nullable List<String> aliases
    ) {
        RelDataType resultType = TypeUtils.getResultType(ctx.typeFactory(), ctx.catalogReader(), sqlType, origins);

        return new FieldsMetadataImpl(sqlType, resultType, origins, aliases);
    }

    /** */
    private FieldsMetadata explainFieldsMetadata(PlanningContext ctx) {
        IgniteTypeFactory factory = ctx.typeFactory();
        RelDataType planStrDataType =
            factory.createSqlType(SqlTypeName.VARCHAR, PRECISION_NOT_SPECIFIED);
        T2<String, RelDataType> planField = new T2<>(ExplainPlan.PLAN_COL_NAME, planStrDataType);
        RelDataType planDataType = factory.createStructType(singletonList(planField));

        return queryFieldsMetadata(ctx, planDataType, null, null);
    }
}
