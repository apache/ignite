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

import java.util.List;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlIdentifier;
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
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.calcite.DistributedCalciteConfiguration;
import org.apache.ignite.internal.processors.query.calcite.prepare.ddl.DdlSqlToCommandConverter;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.sql.IgniteSqlSelectForUpdate;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.AbstractService;
import org.apache.ignite.internal.processors.query.calcite.util.IgniteResource;
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
     *   <li>Validate that the FROM clause is a single table (no JOINs, no subqueries).</li>
     *   <li>Rewrite the SELECT list to append {@code _KEY} as the last column.</li>
     *   <li>Prepare the modified SELECT as a normal {@link MultiStepQueryPlan}.</li>
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

        // Unwrap optional AS alias around the table reference.
        SqlNode from = select.getFrom();

        if (from == null)
            throw new IgniteSQLException(
                "SELECT FOR UPDATE requires a FROM clause",
                IgniteQueryErrorCode.UNSUPPORTED_OPERATION);

        SqlNode fromUnwrapped = from.getKind() == SqlKind.AS ? ((SqlCall)from).operand(0) : from;

        // Reject JOINs.
        if (fromUnwrapped.getKind() == SqlKind.JOIN)
            throw new IgniteSQLException(
                IgniteResource.INSTANCE.selectForUpdateJoinNotSupported().str(),
                IgniteQueryErrorCode.UNSUPPORTED_OPERATION);

        // Reject subqueries and other non-table references.
        if (!(fromUnwrapped instanceof SqlIdentifier))
            throw new IgniteSQLException(
                "SELECT FOR UPDATE is only supported for simple table references",
                IgniteQueryErrorCode.UNSUPPORTED_OPERATION);

        SqlIdentifier tableId = (SqlIdentifier)fromUnwrapped;

        // Extract schema and table names.
        String schemaName;
        String tableName;

        if (tableId.names.size() >= 2) {
            schemaName = tableId.names.get(tableId.names.size() - 2);
            tableName = tableId.names.get(tableId.names.size() - 1);
        }
        else {
            schemaName = ctx.schemaName();
            tableName = tableId.getSimple();
        }

        // Rewrite SELECT list: append _KEY as the last column.
        SqlNodeList origList = select.getSelectList();
        SqlNodeList newList = new SqlNodeList(SqlParserPos.ZERO);

        for (SqlNode col : origList)
            newList.add(col);

        newList.add(new SqlIdentifier(QueryUtils.KEY_FIELD_NAME, SqlParserPos.ZERO));
        newList.add(new SqlIdentifier(QueryUtils.VAL_FIELD_NAME, SqlParserPos.ZERO));
        newList.add(new SqlIdentifier(QueryUtils.VER_FIELD_NAME, SqlParserPos.ZERO));

        SqlSelect modifiedSelect = new SqlSelect(
            SqlParserPos.ZERO,
            null,
            newList,
            select.getFrom(),
            select.getWhere(),
            select.getGroup(),
            select.getHaving(),
            null,
            null,
            select.getOrderList(),
            select.getOffset(),
            select.getFetch(),
            select.getHints()
        );

        // Prepare the modified SELECT as a regular query plan.
        PlanningContext pctx = PlanningContext.builder()
            .parentContext(ctx)
            .query(ctx.query())
            .parameters(ctx.parameters())
            .plannerTimeout(ctx.plannerTimeout())
            .allowTechnicalColumns(true)
            .build();

        MultiStepQueryPlan innerPlan = (MultiStepQueryPlan)prepareQuery(modifiedSelect, pctx);

        int userColCount = innerPlan.fieldsMetadata().rowType().getFieldCount() - 3;

        return new SelectForUpdatePlan(innerPlan, userColCount, forUpdate.waitSeconds(), schemaName, tableName);
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
