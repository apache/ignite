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
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.function.Supplier;
import jdk.vm.ci.code.site.Call;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.ValidationException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.failure.FailureProcessor;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.calcite.prepare.ddl.DdlSqlToCommandConverter;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.schema.SchemaHolder;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.AbstractService;
import org.apache.ignite.internal.processors.query.calcite.util.TypeUtils;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.thread.IgniteThreadFactory;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import org.jetbrains.annotations.Nullable;

import static java.util.Collections.singletonList;
import static org.apache.calcite.rel.type.RelDataType.PRECISION_NOT_SPECIFIED;
import static org.apache.ignite.configuration.IgniteConfiguration.DFLT_THREAD_KEEP_ALIVE_TIME;
import static org.apache.ignite.internal.processors.query.calcite.exec.PlannerHelper.optimize;

/**
 *
 */
@SuppressWarnings("TypeMayBeWeakened")
public class PrepareServiceImpl extends AbstractService {
    /**
     *
     */
    private static final int MAX_PREPARE_THREADS = 4;

    /**
     *
     */
    private final DdlSqlToCommandConverter ddlConverter;

    /**
     *
     */
    private QueryPlanCache qryPlanCache;

    /**
     *
     */
    private SchemaHolder schemaHolder;

    /**
     *
     */
    private FailureProcessor failureProcessor;

    /**
     *
     */
    private IgniteThreadPoolExecutor exec;

    /**
     * @param ctx Kernal.
     */
    public PrepareServiceImpl(GridKernalContext ctx) {
        super(ctx);

        ddlConverter = new DdlSqlToCommandConverter();

        ThreadFactory factory = new IgniteThreadFactory(ctx.igniteInstanceName(), "prepare", ctx.uncaughtExceptionHandler());

        exec = new IgniteThreadPoolExecutor(
            1,
            MAX_PREPARE_THREADS,
            DFLT_THREAD_KEEP_ALIVE_TIME,
            new LinkedBlockingQueue<>(),
            factory);

        exec.allowCoreThreadTimeOut(true);
    }

    /**
     *
     */
    public <T> CompletableFuture<T> submit(Supplier<T> prepareTask) {
        return CompletableFuture.supplyAsync(prepareTask, exec);
    }

    /** {@inheritDoc} */
    @Override public void onStart(GridKernalContext ctx) {
        super.onStart(ctx);
    }

    /** */
    public QueryPlan prepareSingle(SqlNode sqlNode, PlanningContext ctx) {
        try {
            assert single(sqlNode);

            ctx.planner().reset();

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
                    return prepareDml(sqlNode, ctx);

                case EXPLAIN:
                    return prepareExplain(sqlNode, ctx);

                default:
                    throw new IgniteSQLException("Unsupported operation [" +
                        "sqlNodeKind=" + sqlNode.getKind() + "; " +
                        "querySql=\"" + ctx.query() + "\"]", IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
            }
        }
        catch (ValidationException e) {
            throw new IgniteSQLException("Failed to validate query.", IgniteQueryErrorCode.PARSING, e);
        }
    }

    /**
     *
     */
    private QueryPlan prepareDdl(SqlNode sqlNode, PlanningContext ctx) {
        assert sqlNode instanceof SqlDdl : sqlNode == null ? "null" : sqlNode.getClass().getName();

        return new DdlPlan(ddlConverter.convert((SqlDdl)sqlNode, ctx));
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

        return new ExplainPlan(plan, explainFieldsMetadata(ctx));
    }

    /**
     *
     */
    private boolean single(SqlNode sqlNode) {
        return !(sqlNode instanceof SqlNodeList);
    }

    /**
     *
     */
    private QueryPlan prepareQuery(SqlNode sqlNode, PlanningContext ctx) {
        IgnitePlanner planner = ctx.planner();

        // Validate
        ValidationResult validated = planner.validateAndGetTypeMetadata(sqlNode);

        sqlNode = validated.sqlNode();

        IgniteRel igniteRel = optimize(sqlNode, planner, log);

        // Split query plan to query fragments.
        List<Fragment> fragments = new Splitter().go(igniteRel);

        QueryTemplate template = new QueryTemplate(fragments);

        return new MultiStepQueryPlan(template, queryFieldsMetadata(ctx, validated.dataType(), validated.origins()));
    }

    /**
     *
     */
    private QueryPlan prepareDml(SqlNode sqlNode, PlanningContext ctx) throws ValidationException {
        IgnitePlanner planner = ctx.planner();

        // Validate
        sqlNode = planner.validate(sqlNode);

        // Convert to Relational operators graph
        IgniteRel igniteRel = optimize(sqlNode, planner, log);

        // Split query plan to query fragments.
        List<Fragment> fragments = new Splitter().go(igniteRel);

        QueryTemplate template = new QueryTemplate(fragments);

        return new MultiStepDmlPlan(template, queryFieldsMetadata(ctx, igniteRel.getRowType(), null));
    }

    /**
     *
     */
    private FieldsMetadata queryFieldsMetadata(PlanningContext ctx, RelDataType sqlType,
        @Nullable List<List<String>> origins) {
        RelDataType resultType = TypeUtils.getResultType(
            ctx.typeFactory(), ctx.catalogReader(), sqlType, origins);
        return new FieldsMetadataImpl(resultType, origins);
    }

    /**
     *
     */
    private FieldsMetadata explainFieldsMetadata(PlanningContext ctx) {
        IgniteTypeFactory factory = ctx.typeFactory();
        RelDataType planStrDataType =
            factory.createSqlType(SqlTypeName.VARCHAR, PRECISION_NOT_SPECIFIED);
        T2<String, RelDataType> planField = new T2<>(ExplainPlan.PLAN_COL_NAME, planStrDataType);
        RelDataType planDataType = factory.createStructType(singletonList(planField));

        return queryFieldsMetadata(ctx, planDataType, null);
    }

    /** */
    public IgniteThreadPoolExecutor executor() {
        return exec;
    }

    /** */
    public boolean isPrepareDependsOn(SqlNode qry0, SqlNode prev) {
        return prev != null;
//        return prev != null && SqlKind.DDL.contains(prev.getKind());
    }

    /** */
    public boolean isExecuteDependsOn(SqlNode qry0, SqlNode prev) {
        return prev != null;
//        return  prev != null && !(SqlKind.QUERY.contains(qry0)) && !(SqlKind.QUERY.contains(prev));
    }
}
