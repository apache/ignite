/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.rule.logical;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Collect;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.LogicVisitor;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlQuantifyOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.immutables.value.Value;

import static java.util.Objects.requireNonNull;
import static org.apache.calcite.util.Util.last;

/**
 * Transform that converts IN, EXISTS and scalar sub-queries into joins.
 *
 * <p>Sub-queries are represented by {@link RexSubQuery} expressions.
 *
 * <p>A sub-query may or may not be correlated. If a sub-query is correlated,
 * the wrapped {@link RelNode} will contain a {@link RexCorrelVariable} before
 * the rewrite, and the product of the rewrite will be a {@link Correlate}.
 * The Correlate can be removed using {@link RelDecorrelator}.
 *
 * @see CoreRules#FILTER_SUB_QUERY_TO_CORRELATE
 * @see CoreRules#PROJECT_SUB_QUERY_TO_CORRELATE
 * @see CoreRules#JOIN_SUB_QUERY_TO_CORRELATE
 *
 * TODO Revise after https://issues.apache.org/jira/browse/CALCITE-7034
 */
@Value.Enclosing
public class IgniteSubQueryRemoveRule extends RelRule<IgniteSubQueryRemoveRule.Config> implements TransformationRule {
    /** */
    public static final RelOptRule JOIN = Config.JOIN.toRule();

    /** */
    public static final RelOptRule FILTER = Config.FILTER.toRule();

    /** Searches for true input node. Skips projects, etc. */
    private static final OriginalInputShuttle ORIGINAL_INPUT_SHUTTLE = new OriginalInputShuttle();

    /** Creates a SubQueryRemoveRule. */
    protected IgniteSubQueryRemoveRule(Config cfg) {
        super(cfg);

        requireNonNull(cfg.matchHandler());
    }

    /** {@inheritDoc} */
    @Override public void onMatch(RelOptRuleCall call) {
        config.matchHandler().accept(this, call);
    }

    /** */
    protected RexNode apply(
        RexSubQuery e,
        Set<CorrelationId> variablesSet,
        RelOptUtil.Logic logic,
        RelBuilder builder,
        int inputCnt,
        int offset,
        int subQryIdx
    ) {
        switch (e.getKind()) {
            case SCALAR_QUERY:
                return rewriteScalarQuery(e, variablesSet, builder, inputCnt, offset);
            case ARRAY_QUERY_CONSTRUCTOR:
            case MAP_QUERY_CONSTRUCTOR:
            case MULTISET_QUERY_CONSTRUCTOR:
                return rewriteCollection(e, variablesSet, builder, inputCnt, offset);
            case SOME:
                return rewriteSome(e, variablesSet, builder, subQryIdx);
            case IN:
                return rewriteIn(e, variablesSet, logic, builder, offset, subQryIdx);
            case EXISTS:
                return rewriteExists(e, variablesSet, logic, builder);
            case UNIQUE:
                return rewriteUnique(e, builder);
            default:
                throw new AssertionError(e.getKind());
        }
    }

    /**
     * Rewrites a scalar sub-query into an
     * {@link org.apache.calcite.rel.core.Aggregate}.
     * @param e Scalar sub-query to rewrite
     * @param variablesSet A set of variables used by a relational
     *                     expression of the specified RexSubQuery
     * @param builder Builder
     * @param offset Offset to shift {@link RexInputRef}
     * @return Expression that may be used to replace the RexSubQuery
     */
    private static RexNode rewriteScalarQuery(
        RexSubQuery e,
        Set<CorrelationId> variablesSet,
        RelBuilder builder,
        int inputCnt,
        int offset
    ) {
        builder.push(e.rel);

        RelMetadataQuery mq = e.rel.getCluster().getMetadataQuery();
        Boolean unique = mq.areColumnsUnique(builder.peek(), ImmutableBitSet.of());

        if (unique == null || !unique)
            builder.aggregate(builder.groupKey(), builder.aggregateCall(SqlStdOperatorTable.SINGLE_VALUE, builder.field(0)));

        builder.join(JoinRelType.LEFT, builder.literal(true), variablesSet);

        return field(builder, inputCnt, offset);
    }

    /**
     * Rewrites a sub-query into a {@link org.apache.calcite.rel.core.Collect}.
     *
     * @param e Sub-query to rewrite
     * @param variablesSet A set of variables used by a relational expression of the specified RexSubQuery
     * @param builder Builder
     * @param offset Offset to shift {@link RexInputRef}
     * @return Expression that may be used to replace the RexSubQuery
     */
    private static RexNode rewriteCollection(
        RexSubQuery e,
        Set<CorrelationId> variablesSet,
        RelBuilder builder,
        int inputCnt,
        int offset
    ) {
        builder.push(e.rel);
        builder.push(Collect.create(builder.build(), e.getKind(), "x"));
        builder.join(JoinRelType.INNER, builder.literal(true), variablesSet);

        return field(builder, inputCnt, offset);
    }

    /**
     * Rewrites a SOME sub-query into a {@link Join}.
     *
     * @param e SOME sub-query to rewrite
     * @param builder Builder
     * @param subQryIdx sub-query index in multiple sub-queries
     * @return Expression that may be used to replace the RexSubQuery
     */
    private static RexNode rewriteSome(RexSubQuery e, Set<CorrelationId> variablesSet, RelBuilder builder, int subQryIdx) {
        // If the sub-query is guaranteed empty, just return FALSE.
        RelMetadataQuery mq = e.rel.getCluster().getMetadataQuery();

        if (RelMdUtil.isRelDefinitelyEmpty(mq, e.rel))
            return builder.getRexBuilder().makeLiteral(Boolean.FALSE, e.getType(), true);

        // Most general case, where the left and right keys might have nulls, and
        // caller requires 3-valued logic return.
        //
        // select e.deptno, e.deptno < some (select deptno from emp) as v
        // from emp as e
        //
        // becomes
        //
        // select e.deptno,
        //   case
        //   when q.c = 0 then false // sub-query is empty
        //   when (e.deptno < q.m) is true then true
        //   when q.c > q.d then unknown // sub-query has at least one null
        //   else e.deptno < q.m
        //   end as v
        // from emp as e
        // cross join (
        //   select max(deptno) as m, count(*) as c, count(deptno) as d
        //   from emp) as q
        //
        SqlQuantifyOperator op = (SqlQuantifyOperator)e.op;

        switch (op.comparisonKind) {
            case GREATER_THAN_OR_EQUAL:
            case LESS_THAN_OR_EQUAL:
            case LESS_THAN:
            case GREATER_THAN:
            case NOT_EQUALS:
                break;

            default:
                // "SOME =" should have been rewritten into IN.
                throw new AssertionError("unexpected " + op);
        }

        RexNode caseRexNode;
        RexNode literalFalse = builder.literal(false);
        RexNode literalTrue = builder.literal(true);
        RexLiteral literalUnknown = builder.getRexBuilder().makeNullLiteral(literalFalse.getType());

        SqlAggFunction minMax = op.comparisonKind == SqlKind.GREATER_THAN || op.comparisonKind == SqlKind.GREATER_THAN_OR_EQUAL
            ? SqlStdOperatorTable.MIN
            : SqlStdOperatorTable.MAX;

        String qAlias = "q";

        if (subQryIdx != 0)
            qAlias = "q" + subQryIdx;

        if (variablesSet.isEmpty()) {
            switch (op.comparisonKind) {
                case GREATER_THAN_OR_EQUAL:
                case LESS_THAN_OR_EQUAL:
                case LESS_THAN:
                case GREATER_THAN:
                    // for non-correlated case queries such as
                    // select e.deptno, e.deptno < some (select deptno from emp) as v
                    // from emp as e
                    //
                    // becomes
                    //
                    // select e.deptno,
                    //   case
                    //   when q.c = 0 then false // sub-query is empty
                    //   when (e.deptno < q.m) is true then true
                    //   when q.c > q.d then unknown // sub-query has at least one null
                    //   else e.deptno < q.m
                    //   end as v
                    // from emp as e
                    // cross join (
                    //   select max(deptno) as m, count(*) as c, count(deptno) as d
                    //   from emp) as q
                    builder.push(e.rel).aggregate(
                        builder.groupKey(),
                        builder.aggregateCall(minMax, builder.field(0)).as("m"),
                        builder.count(false, "c"),
                        builder.count(false, "d", builder.field(0))
                    ).as(qAlias).join(JoinRelType.INNER);

                    caseRexNode = builder.call(
                        SqlStdOperatorTable.CASE,
                        builder.equals(builder.field(qAlias, "c"), builder.literal(0)),
                        literalFalse,
                        builder.call(
                            SqlStdOperatorTable.IS_TRUE,
                            builder.call(RexUtil.op(op.comparisonKind), e.operands.get(0), builder.field(qAlias, "m"))
                        ),
                        literalTrue,
                        builder.greaterThan(builder.field(qAlias, "c"), builder.field(qAlias, "d")),
                        literalUnknown,
                        builder.call(RexUtil.op(op.comparisonKind), e.operands.get(0), builder.field(qAlias, "m"))
                    );
                    break;

                case NOT_EQUALS:
                    // for non-correlated case queries such as
                    // select e.deptno, e.deptno <> some (select deptno from emp) as v
                    // from emp as e
                    //
                    // becomes
                    //
                    // select e.deptno,
                    //   case
                    //   when q.c = 0 then false // sub-query is empty
                    //   when e.deptno is null then unknown
                    //   when q.c <> q.d && q.d <= 1 then e.deptno != m || unknown
                    //   when q.d = 1
                    //     then e.deptno != m // sub-query has the distinct result
                    //   else true
                    //   end as v
                    // from emp as e
                    // cross join (
                    //   select count(*) as c, count(deptno) as d, max(deptno) as m
                    //   from (select distinct deptno from emp)) as q
                    builder.push(e.rel);

                    builder.distinct()
                        .aggregate(
                            builder.groupKey(),
                            builder.count(false, "c"),
                            builder.count(false, "d", builder.field(0)),
                            builder.max(builder.field(0)).as("m")
                        ).as(qAlias).join(JoinRelType.INNER);

                    caseRexNode = builder.call(
                        SqlStdOperatorTable.CASE,
                        builder.equals(builder.field("c"), builder.literal(0)),
                        literalFalse,
                        builder.isNull(e.getOperands().get(0)),
                        literalUnknown,
                        builder.and(
                            builder.notEquals(builder.field("d"), builder.field("c")),
                            builder.lessThanOrEqual(builder.field("d"),
                                builder.literal(1))
                        ),
                        builder.or(builder.notEquals(e.operands.get(0), builder.field(qAlias, "m")), literalUnknown),
                        builder.equals(builder.field("d"), builder.literal(1)),
                        builder.notEquals(e.operands.get(0), builder.field(qAlias, "m")),
                        literalTrue
                    );
                    break;

                default:
                    throw new AssertionError("not possible - per above check");
            }
        }
        else {
            String indicator = "trueLiteral";

            List<RexNode> parentQryFields = new ArrayList<>();

            switch (op.comparisonKind) {
                case GREATER_THAN_OR_EQUAL:
                case LESS_THAN_OR_EQUAL:
                case LESS_THAN:
                case GREATER_THAN:
                    // for correlated case queries such as
                    //
                    // select e.deptno, e.deptno < some (
                    //   select deptno from emp where emp.name = e.name) as v
                    // from emp as e
                    //
                    // becomes
                    //
                    // select e.deptno,
                    //   case
                    //   when indicator is null then false // sub-query is empty for corresponding corr value
                    //   when q.c = 0 then false // sub-query is empty
                    //   when (e.deptno < q.m) is true then true
                    //   when q.c > q.d then unknown // sub-query has at least one null
                    //   else e.deptno < q.m
                    //   end as v
                    // from emp as e
                    // left outer join (
                    //   select name, max(deptno) as m, count(*) as c, count(deptno) as d,
                    //       "alwaysTrue" as indicator
                    //   from emp group by name) as q on e.name = q.name
                    builder.push(e.rel).aggregate(builder.groupKey(),
                        builder.aggregateCall(minMax, builder.field(0)).as("m"),
                        builder.count(false, "c"),
                        builder.count(false, "d", builder.field(0)));

                    parentQryFields.addAll(builder.fields());
                    parentQryFields.add(builder.alias(literalTrue, indicator));
                    builder.project(parentQryFields).as(qAlias);
                    builder.join(JoinRelType.LEFT, literalTrue, variablesSet);

                    caseRexNode = builder.call(
                        SqlStdOperatorTable.CASE,
                        builder.isNull(builder.field(qAlias, indicator)),
                        literalFalse,
                        builder.equals(builder.field(qAlias, "c"), builder.literal(0)),
                        literalFalse,
                        builder.call(
                            SqlStdOperatorTable.IS_TRUE,
                            builder.call(RexUtil.op(op.comparisonKind), e.operands.get(0), builder.field(qAlias, "m"))
                        ),
                        literalTrue,
                        builder.greaterThan(builder.field(qAlias, "c"), builder.field(qAlias, "d")),
                        literalUnknown,
                        builder.call(RexUtil.op(op.comparisonKind), e.operands.get(0), builder.field(qAlias, "m"))
                    );
                    break;

                case NOT_EQUALS:
                    // for correlated case queries such as
                    //
                    // select e.deptno, e.deptno <> some (
                    //   select deptno from emp where emp.name = e.name) as v
                    // from emp as e
                    //
                    // becomes
                    //
                    // select e.deptno,
                    //   case
                    //   when indicator is null
                    //     then false // sub-query is empty for corresponding corr value
                    //   when q.c = 0 then false // sub-query is empty
                    //   when e.deptno is null then unknown
                    //   when q.c <> q.d && q.dd <= 1
                    //     then e.deptno != m || unknown
                    //   when q.dd = 1
                    //     then e.deptno != m // sub-query has the distinct result
                    //   else true
                    //   end as v
                    // from emp as e
                    // left outer join (
                    //   select name, count(*) as c, count(deptno) as d, count(distinct deptno) as dd,
                    //       max(deptno) as m, "alwaysTrue" as indicator
                    //   from emp group by name) as q on e.name = q.name

                    // Additional details on the `q.c <> q.d && q.dd <= 1` clause:
                    // the q.c <> q.d comparison identifies if there are any null values,
                    // since count(*) counts null values and count(deptno) does not.
                    // if there's no null value, c should be equal to d.
                    // the q.dd <= 1 part means: true if there is at most one non-null value
                    // so this clause means:
                    // "if there are any null values and there is at most one non-null value".
                    builder.push(e.rel).aggregate(
                        builder.groupKey(),
                        builder.count(false, "c"),
                        builder.count(false, "d", builder.field(0)),
                        builder.count(true, "dd", builder.field(0)),
                        builder.max(builder.field(0)).as("m")
                    );

                    parentQryFields.addAll(builder.fields());
                    parentQryFields.add(builder.alias(literalTrue, indicator));

                    builder.project(parentQryFields).as(qAlias); // TODO use projectPlus
                    builder.join(JoinRelType.LEFT, literalTrue, variablesSet);

                    caseRexNode = builder.call(
                        SqlStdOperatorTable.CASE,
                        builder.isNull(builder.field(qAlias, indicator)),
                        literalFalse,
                        builder.equals(builder.field("c"), builder.literal(0)),
                        literalFalse,
                        builder.isNull(e.getOperands().get(0)),
                        literalUnknown,
                        builder.and(
                            builder.notEquals(builder.field("d"), builder.field("c")),
                            builder.lessThanOrEqual(builder.field("dd"), builder.literal(1))
                        ),
                        builder.or(builder.notEquals(e.operands.get(0), builder.field(qAlias, "m")), literalUnknown),
                        builder.equals(builder.field("dd"), builder.literal(1)),
                        builder.notEquals(e.operands.get(0), builder.field(qAlias, "m")),
                        literalTrue
                    );
                    break;

                default:
                    throw new AssertionError("not possible - per above check");
            }
        }

        // CASE statement above is created with nullable boolean type, but it might
        // not be correct.  If the original sub-query node's type is not nullable it
        // is guaranteed for case statement to not produce NULLs. Therefore to avoid
        // planner complaining we need to add cast.  Note that nullable type is
        // created due to the MIN aggregate call, since there is no GROUP BY.
        if (!e.getType().isNullable())
            return builder.cast(caseRexNode, e.getType().getSqlTypeName());

        return caseRexNode;
    }

    /**
     * Rewrites an EXISTS RexSubQuery into a {@link Join}.
     *
     * @param e            EXISTS sub-query to rewrite
     * @param variablesSet A set of variables used by a relational expression of the specified RexSubQuery
     * @param logic        Logic for evaluating
     * @param builder      Builder
     * @return Expression that may be used to replace the RexSubQuery
     */
    private static RexNode rewriteExists(
        RexSubQuery e,
        Set<CorrelationId> variablesSet,
        RelOptUtil.Logic logic,
        RelBuilder builder
    ) {
        // If the sub-query is guaranteed never empty, just return TRUE.
        RelMetadataQuery mq = e.rel.getCluster().getMetadataQuery();

        if (RelMdUtil.isRelDefinitelyNotEmpty(mq, e.rel))
            return builder.literal(true);

        if (RelMdUtil.isRelDefinitelyEmpty(mq, e.rel))
            return builder.literal(false);

        builder.push(e.rel);
        builder.project(builder.alias(builder.literal(true), "i"));

        if (logic == RelOptUtil.Logic.TRUE) {
            // Handles queries with single EXISTS in filter condition:
            // select e.deptno from emp as e
            // where exists (select deptno from emp)
            builder.aggregate(builder.groupKey(0));
            builder.as("dt");
            builder.join(JoinRelType.INNER, builder.literal(true), variablesSet);

            return builder.literal(true);
        }
        else
            builder.distinct();

        builder.as("dt");
        builder.join(JoinRelType.LEFT, builder.literal(true), variablesSet);

        return builder.isNotNull(last(builder.fields()));
    }

    /**
     * Rewrites a UNIQUE RexSubQuery into an EXISTS RexSubQuery.
     *
     * <p>For example, rewrites the UNIQUE sub-query:
     *
     * <pre>{@code
     * UNIQUE (SELECT PUBLISHED_IN
     * FROM BOOK
     * WHERE AUTHOR_ID = 3)
     * }</pre>
     *
     * <p>to the following EXISTS sub-query:
     *
     * <pre>{@code
     * NOT EXISTS (
     *   SELECT * FROM (
     *     SELECT PUBLISHED_IN
     *     FROM BOOK
     *     WHERE AUTHOR_ID = 3
     *   ) T
     *   WHERE (T.PUBLISHED_IN) IS NOT NULL
     *   GROUP BY T.PUBLISHED_IN
     *   HAVING COUNT(*) > 1
     * )
     * }</pre>
     *
     * @param e       UNIQUE sub-query to rewrite
     * @param builder Builder
     * @return Expression that may be used to replace the RexSubQuery
     */
    private static RexNode rewriteUnique(RexSubQuery e, RelBuilder builder) {
        // if sub-query always return unique value.
        RelMetadataQuery mq = e.rel.getCluster().getMetadataQuery();

        Boolean isUnique = mq.areRowsUnique(e.rel, true);

        if (isUnique != null && isUnique)
            return builder.getRexBuilder().makeLiteral(true);

        builder.push(e.rel);

        List<RexNode> notNullCondition = builder.fields().stream().map(builder::isNotNull).collect(Collectors.toList());

        builder.filter(notNullCondition)
            .aggregate(builder.groupKey(builder.fields()), builder.countStar("c"))
            .filter(builder.greaterThan(last(builder.fields()), builder.literal(1)));

        RelNode relNode = builder.build();

        return builder.call(SqlStdOperatorTable.NOT, RexSubQuery.exists(relNode));
    }

    /**
     * Rewrites an IN RexSubQuery into a {@link Join}.
     *
     * @param e IN sub-query to rewrite
     * @param variablesSet A set of variables used by a relational expression of the specified RexSubQuery
     * @param logic Logic for evaluating
     * @param builder Builder
     * @param offset Offset to shift {@link RexInputRef}
     * @param subQryIdx sub-query index in multiple sub-queries
     *
     * @return Expression that may be used to replace the RexSubQuery
     */
    private static RexNode rewriteIn(
        RexSubQuery e,
        Set<CorrelationId> variablesSet,
        RelOptUtil.Logic logic,
        RelBuilder builder,
        int offset,
        int subQryIdx
    ) {
        // If the sub-query is guaranteed empty, just return FALSE.
        RelMetadataQuery mq = e.rel.getCluster().getMetadataQuery();

        if (RelMdUtil.isRelDefinitelyEmpty(mq, e.rel))
            return builder.getRexBuilder().makeLiteral(Boolean.FALSE, e.getType(), true);

        // Most general case, where the left and right keys might have nulls, and
        // caller requires 3-valued logic return.
        //
        // select e.deptno, e.deptno in (select deptno from emp)
        // from emp as e
        //
        // becomes
        //
        // select e.deptno,
        //   case
        //   when ct.c = 0 then false
        //   when e.deptno is null then null
        //   when dt.i is not null then true
        //   when ct.ck < ct.c then null
        //   else false
        //   end
        // from emp as e
        // left join (
        //   (select count(*) as c, count(deptno) as ck from emp) as ct
        //   cross join (select distinct deptno, true as i from emp)) as dt
        //   on e.deptno = dt.deptno
        //
        // If keys are not null we can remove "ct" and simplify to
        //
        // select e.deptno,
        //   case
        //   when dt.i is not null then true
        //   else false
        //   end
        // from emp as e
        // left join (select distinct deptno, true as i from emp) as dt
        //   on e.deptno = dt.deptno
        //
        // We could further simplify to
        //
        // select e.deptno,
        //   dt.i is not null
        // from emp as e
        // left join (select distinct deptno, true as i from emp) as dt
        //   on e.deptno = dt.deptno
        //
        // but have not yet.
        //
        // If the logic is TRUE we can just kill the record if the condition
        // evaluates to FALSE or UNKNOWN. Thus the query simplifies to an inner
        // join:
        //
        // select e.deptno,
        //   true
        // from emp as e
        // inner join (select distinct deptno from emp) as dt
        //   on e.deptno = dt.deptno
        //
        builder.push(e.rel);

        List<RexNode> fields = new ArrayList<>(builder.fields());

        // for the case when IN has only literal operands, it may be handled
        // in the simpler way:
        //
        // select e.deptno, 123456 in (select deptno from emp)
        // from emp as e
        //
        // becomes
        //
        // select e.deptno,
        //   case
        //   when dt.c IS NULL THEN FALSE
        //   when e.deptno IS NULL THEN NULL
        //   when dt.cs IS FALSE THEN NULL
        //   when dt.cs IS NOT NULL THEN TRUE
        //   else false
        //   end
        // from emp AS e
        // cross join (
        //   select distinct deptno is not null as cs, count(*) as c
        //   from emp
        //   where deptno = 123456 or deptno is null or e.deptno is null
        //   order by cs desc limit 1) as dt
        //
        String ctAlias = "ct";

        if (subQryIdx != 0)
            ctAlias = "ct" + subQryIdx;

        boolean allLiterals = RexUtil.allLiterals(e.getOperands());
        List<RexNode> expressionOperands = new ArrayList<>(e.getOperands());

        List<RexNode> keyIsNulls = e.getOperands().stream()
            .filter(operand -> operand.getType().isNullable())
            .map(builder::isNull)
            .collect(Collectors.toList());

        RexLiteral trueLiteral = builder.literal(true);
        RexLiteral falseLiteral = builder.literal(false);
        RexLiteral unknownLiteral = builder.getRexBuilder().makeNullLiteral(trueLiteral.getType());

        if (allLiterals) {
            List<RexNode> conditions = Pair.zip(expressionOperands, fields).stream()
                .map(pair -> builder.equals(pair.left, pair.right))
                .collect(Collectors.toList());

            switch (logic) {
                case TRUE:
                case TRUE_FALSE:
                    builder.filter(conditions);
                    builder.project(builder.alias(trueLiteral, "cs"));
                    builder.distinct();
                    break;
                default:
                    List<RexNode> isNullOperands = fields.stream().map(builder::isNull).collect(Collectors.toList());

                    // uses keyIsNulls conditions in the filter to avoid empty results
                    isNullOperands.addAll(keyIsNulls);

                    builder.filter(builder.or(builder.and(conditions), builder.or(isNullOperands)));

                    RexNode project = builder.and(fields.stream().map(builder::isNotNull).collect(Collectors.toList()));

                    builder.project(builder.alias(project, "cs"));

                    if (variablesSet.isEmpty())
                        builder.aggregate(builder.groupKey(builder.field("cs")), builder.count(false, "c"));
                    else
                        builder.distinct();

                    // sorts input with desc order since we are interested
                    // only in the case when one of the values is true.
                    // When true value is absent then we are interested
                    // only in false value.
                    builder.sortLimit(0, 1, ImmutableList.of(builder.desc(builder.field("cs"))));
            }

            // clears expressionOperands and fields lists since
            // all expressions were used in the filter
            expressionOperands.clear();
            fields.clear();
        }
        else {
            switch (logic) {
                case TRUE:
                    builder.aggregate(builder.groupKey(fields));
                    break;
                case TRUE_FALSE_UNKNOWN:
                case UNKNOWN_AS_TRUE:
                    // Builds the cross join
                    builder.aggregate(builder.groupKey(),
                        builder.count(false, "c"),
                        builder.count(builder.fields()).as("ck"));

                    builder.as(ctAlias);

                    if (!variablesSet.isEmpty())
                        builder.join(JoinRelType.LEFT, trueLiteral, variablesSet);
                    else
                        builder.join(JoinRelType.INNER, trueLiteral, variablesSet);

                    offset += 2;

                    builder.push(e.rel);
                    // fall through
                default:
                    builder.aggregate(builder.groupKey(fields),
                        builder.literalAgg(true).as("i"));
            }
        }

        String dtAlias = "dt";

        if (subQryIdx != 0)
            dtAlias = "dt" + subQryIdx;

        builder.as(dtAlias);

        int refOffset = offset;

        List<RexNode> conditions = Pair.zip(expressionOperands, builder.fields()).stream()
            .map(pair -> builder.equals(pair.left, RexUtil.shift(pair.right, refOffset)))
            .collect(Collectors.toList());

        if (RelOptUtil.Logic.TRUE == logic) {
            builder.join(JoinRelType.INNER, builder.and(conditions), variablesSet);

            return trueLiteral;
        }

        // Now the left join
        builder.join(JoinRelType.LEFT, builder.and(conditions), variablesSet);

        ImmutableList.Builder<RexNode> operands = ImmutableList.builder();
        RexLiteral b = trueLiteral;

        switch (logic) {
            case TRUE_FALSE_UNKNOWN:
                b = unknownLiteral;
                // fall through
            case UNKNOWN_AS_TRUE:
                if (allLiterals) {
                    // Considers case when right side of IN is empty
                    // for the case of non-correlated sub-queries
                    if (variablesSet.isEmpty())
                        operands.add(builder.isNull(builder.field(dtAlias, "c")), falseLiteral);

                    operands.add(builder.equals(builder.field(dtAlias, "cs"), falseLiteral), b);
                }
                else
                    operands.add(builder.equals(builder.field(ctAlias, "c"), builder.literal(0)), falseLiteral);
                break;
            default:
                break;
        }

        if (!keyIsNulls.isEmpty())
            operands.add(builder.or(keyIsNulls), unknownLiteral);

        if (allLiterals)
            operands.add(builder.isNotNull(builder.field(dtAlias, "cs")), trueLiteral);
        else
            operands.add(builder.isNotNull(last(builder.fields())), trueLiteral);

        if (!allLiterals) {
            switch (logic) {
                case TRUE_FALSE_UNKNOWN:
                case UNKNOWN_AS_TRUE:
                    operands.add(builder.lessThan(builder.field(ctAlias, "ck"), builder.field(ctAlias, "c")), b);
                    break;
                default:
                    break;
            }
        }

        operands.add(falseLiteral);

        return builder.call(SqlStdOperatorTable.CASE, operands.build());
    }

    /** Returns a reference to a particular field, by offset, across several inputs on a {@link RelBuilder}'s stack. */
    private static RexInputRef field(RelBuilder builder, int inputCnt, int offset) {
        for (int inputOrdinal = 0; ; ) {
            RelNode r = builder.peek(inputCnt, inputOrdinal);

            if (offset < r.getRowType().getFieldCount())
                return builder.field(inputCnt, inputOrdinal, offset);

            ++inputOrdinal;

            offset -= r.getRowType().getFieldCount();
        }
    }

    /**
     * Returns a list of expressions that project the first {@code fieldCount} fields of the top input
     * on a {@link RelBuilder}'s stack.
     */
    private static List<RexNode> fields(RelBuilder builder, int fieldCnt) {
        List<RexNode> projects = new ArrayList<>();

        for (int i = 0; i < fieldCnt; i++)
            projects.add(builder.field(i));

        return projects;
    }

    /** */
    private static void matchFilter(IgniteSubQueryRemoveRule rule, RelOptRuleCall call) {
        Filter filter = call.rel(0);
        RelBuilder builder = call.builder();
        // Keeps original field numbers of correlate's row type.
        Map<Integer, Integer> corrFldCntMapping = new HashMap<>();

        builder.push(filter.getInput());

        int cnt = 0;

        RexNode condition = filter.getCondition();

        while (true) {
            final RexSubQuery subQry = RexUtil.SubQueryFinder.find(condition);

            if (subQry == null) {
                assert cnt > 0;
                break;
            }

            ++cnt;

            RelOptUtil.Logic logic = LogicVisitor.find(RelOptUtil.Logic.TRUE, ImmutableList.of(condition), subQry);
            Set<CorrelationId> variablesSet = RelOptUtil.getVariablesUsed(subQry.rel);

            RexSubQuery subQryReplaced = subQry;

            // Filter without variables could be handled before this change, we do not want
            // to break it yet for compatibility reason.
            if (!filter.getVariablesSet().isEmpty()) {
                // Only consider the correlated variables which originated from this sub-query level.
                variablesSet.retainAll(filter.getVariablesSet());

                rebuildCorrFldCntMap(corrFldCntMapping, filter, true);
            }

            if (!variablesSet.isEmpty() && (subQry.getKind() == SqlKind.IN || subQry.getKind() == SqlKind.EXISTS
                || subQry.getKind() == SqlKind.SOME)
            ) {
                CorrelationId id = Iterables.getOnlyElement(variablesSet);
                RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
                RelNode filterInput = originalInput(filter.getInput());

                subQryReplaced = subQry.clone(subQryReplaced.rel.accept(new RelHomogeneousShuttle() {
                    private final RexShuttle rexShuttle = new RexShuttle() {
                        @Override public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
                            if (!(fieldAccess.getReferenceExpr() instanceof RexCorrelVariable)
                                || !((RexCorrelVariable)fieldAccess.getReferenceExpr()).id.equals(id))
                                return super.visitFieldAccess(fieldAccess);

                            if (corrFldCntMapping.isEmpty())
                                rebuildCorrFldCntMap(corrFldCntMapping, call.getPlanner().getRoot(), false);

                            assert corrFldCntMapping.get(id.getId()) != null;

                            // Filter's input is put to the left join shoulder at the rewritting.
                            int offset = corrFldCntMapping.get(id.getId()) - filterInput.getRowType().getFieldCount();

                            if (offset == 0)
                                return super.visitFieldAccess(fieldAccess);

                            RexNode newCorr = rexBuilder.makeCorrel(subQry.rel.getRowType(), id);

                            int oldIdx = fieldAccess.getField().getIndex();

                            assert oldIdx >= offset;

                            return rexBuilder.makeFieldAccess(newCorr, oldIdx - offset);
                        }
                    };

                    @Override public RelNode visit(RelNode other) {
                        return super.visit(other).accept(rexShuttle);
                    }
                }));
            }

            RexNode target = rule.apply(subQryReplaced, variablesSet, logic, builder, 1, builder.peek().getRowType().getFieldCount(), cnt);
            RexShuttle shuttle = new ReplaceSubQueryShuttle(subQry, target);

            condition = condition.accept(shuttle);
        }

        builder.filter(condition);
        builder.project(fields(builder, filter.getRowType().getFieldCount()));

        RelNode result = builder.build();

        call.transformTo(result);
    }

    /** */
    private static void rebuildCorrFldCntMap(Map<Integer, Integer> outMap, RelNode rootRel, boolean single) {
        rootRel.accept(new RelShuttleImpl() {
            @Override public RelNode visit(RelNode other) {
                if (other instanceof HepRelVertex) {
                    other = ((HepRelVertex)other).getCurrentRel();

                    if (other instanceof LogicalFilter)
                        visit((LogicalFilter)other);
                    else if (other instanceof LogicalCorrelate)
                        visit((LogicalCorrelate)other);

                    return super.visit(other);
                }

                return super.visit(other);
            }

            @Override public RelNode visit(LogicalFilter filter) {
                RelNode input = originalInput(filter);

                for (CorrelationId corrId : filter.getVariablesSet())
                    addCorrelate(corrId, input.getRowType().getFieldCount());

                if (single)
                    return null;

                return super.visit(filter);
            }

            @Override public RelNode visit(LogicalCorrelate correlate) {
                RelNode input = originalInput(correlate);

                addCorrelate(correlate.getCorrelationId(), input.getRowType().getFieldCount());

                return super.visit(correlate);
            }

            private void addCorrelate(CorrelationId corrId, int srcFldsCnt) {
                assert outMap.get(corrId.getId()) == null || outMap.get(corrId.getId()) == srcFldsCnt;

                outMap.putIfAbsent(corrId.getId(), srcFldsCnt);
            }
        });
    }

    /** */
    private static RelNode originalInput(RelNode node) {
        try {
            node.accept(ORIGINAL_INPUT_SHUTTLE);
        }
        catch (Util.FoundOne found) {
            return (RelNode)found.getNode();
        }

        throw new IllegalStateException("No input node found for " + node);
    }

    /** */
    private static void matchJoin(IgniteSubQueryRemoveRule rule, RelOptRuleCall call) {
        Join join = call.rel(0);
        RelBuilder builder = call.builder();

        RexSubQuery e = requireNonNull(RexUtil.SubQueryFinder.find(join.getCondition()));
        RelOptUtil.Logic logic = LogicVisitor.find(RelOptUtil.Logic.TRUE, ImmutableList.of(join.getCondition()), e);

        ImmutableBitSet inputSet = RelOptUtil.InputFinder.bits(e.getOperands(), null);
        int nFieldsLeft = join.getLeft().getRowType().getFieldCount();
        int nFieldsRight = join.getRight().getRowType().getFieldCount();
        Set<CorrelationId> variablesSet = RelOptUtil.getVariablesUsed(e.rel);

        if (!variablesSet.isEmpty()) {
            CorrelationId id = Iterables.getOnlyElement(variablesSet);
            inputSet = ImmutableBitSet.union(List.of(RelOptUtil.correlationColumns(id, e.rel), inputSet));
        }

        boolean inputIntersectsRightSide = inputSet.intersects(ImmutableBitSet.range(nFieldsLeft, nFieldsLeft + nFieldsRight));
        boolean inputIntersectsLeftSide = inputSet.intersects(ImmutableBitSet.range(0, nFieldsLeft));

        if (inputIntersectsLeftSide && inputIntersectsRightSide) {
            // The current existential rewrite needs to make join with one side of the origin join and
            // generate a new condition to replace the on clause. But for RexNode whose operands are
            // on either side of the join, we can't push them into join. So this rewriting is not
            // supported.
            return;
        }

        builder.push(join.getLeft());

        if (inputIntersectsLeftSide) {
            RexNode target = rule.apply(e, variablesSet, logic, builder, 1, nFieldsLeft, 0);
            RexShuttle shuttle = new ReplaceSubQueryShuttle(e, target);
            RexNode newCond = shuttle.apply(RexUtil.shift(join.getCondition(), nFieldsLeft, builder.fields().size() - nFieldsLeft));

            builder.push(join.getRight());
            builder.join(join.getJoinType(), newCond);

            int nFields = builder.fields().size();

            ImmutableList<RexNode> fields = builder.fields(ImmutableBitSet.range(0, nFieldsLeft)
                .union(ImmutableBitSet.range(nFields - nFieldsRight, nFields)));

            builder.project(fields);
        }
        else {
            builder.push(join.getRight());

            RexSubQuery subQry = e;

            if (!variablesSet.isEmpty()) {
                // Original correlates reference joint row type, but we are about to create
                // new join of original right side and correlated sub-query. Therefore we have
                // to adjust correlated variables int following way:
                //      1) new correlation variable must reference row type of right side only
                //      2) field index must be shifted on the size of the left side
                CorrelationId id = Iterables.getOnlyElement(variablesSet);

                subQry = e.clone(e.rel.accept(new RelHomogeneousShuttle() {
                    private final int offset = join.getLeft().getRowType().getFieldCount();

                    private final RexBuilder rexBuilder = join.getRight().getCluster().getRexBuilder();

                    private final RexShuttle rexShuttle = new RexShuttle() {
                        @Override public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
                            if (!(fieldAccess.getReferenceExpr() instanceof RexCorrelVariable)
                                || !((RexCorrelVariable)fieldAccess.getReferenceExpr()).id.equals(id))
                                return super.visitFieldAccess(fieldAccess);

                            RexNode updatedCorrelation = rexBuilder.makeCorrel(join.getRight().getRowType(), id);

                            int oldIdx = fieldAccess.getField().getIndex();
                            return rexBuilder.makeFieldAccess(updatedCorrelation, oldIdx - offset);
                        }
                    };

                    @Override public RelNode visit(RelNode other) {
                        RelNode next = super.visit(other);
                        return next.accept(rexShuttle);
                    }
                }));
            }

            int nFields = join.getRowType().getFieldCount();
            RexNode target = rule.apply(subQry, variablesSet, logic, builder, 2, nFields, 0);
            RexShuttle shuttle = new ReplaceSubQueryShuttle(e, target);

            builder.join(join.getJoinType(), shuttle.apply(join.getCondition()));
            builder.project(fields(builder, nFields));
        }

        call.transformTo(builder.build());
    }

    /** Shuttle that replaces occurrences of a given {@link org.apache.calcite.rex.RexSubQuery} with a replacement expression. */
    private static class ReplaceSubQueryShuttle extends RexShuttle {
        /** */
        private final RexSubQuery subQry;

        /** */
        private final RexNode replacement;

        /** */
        private ReplaceSubQueryShuttle(RexSubQuery subQry, RexNode replacement) {
            this.subQry = subQry;
            this.replacement = replacement;
        }

        /** {@inheritDoc} */
        @Override public RexNode visitSubQuery(RexSubQuery subQry) {
            return subQry.equals(this.subQry) ? replacement : subQry;
        }
    }

    /** */
    private static class OriginalInputShuttle extends RelHomogeneousShuttle {
        /** {@inheritDoc} */
        @Override public RelNode visit(RelNode other) {
            if (other instanceof HepRelVertex)
                other = ((HepRelVertex)other).getCurrentRel();

            if (other.getInputs().isEmpty() || other.getInputs().size() > 1)
                throw new Util.FoundOne(other);

            return super.visit(other);
        }
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface Config extends RelRule.Config {
        /** */
        Config JOIN = ImmutableIgniteSubQueryRemoveRule.Config.builder()
            .withMatchHandler(IgniteSubQueryRemoveRule::matchJoin)
            .build()
            .withOperandSupplier(b ->
                b.operand(Join.class)
                    .predicate(RexUtil.SubQueryFinder::containsSubQuery)
                    .anyInputs())
            .withDescription("SubQueryRemoveRule:Join");

        /** */
        Config FILTER = ImmutableIgniteSubQueryRemoveRule.Config.builder()
            .withMatchHandler(IgniteSubQueryRemoveRule::matchFilter)
            .build()
            .withOperandSupplier(b ->
                b.operand(Filter.class)
                    .predicate(RexUtil.SubQueryFinder::containsSubQuery).anyInputs())
            .withDescription("SubQueryRemoveRule:Filter");

        /** {@inheritDoc} */
        @Override default IgniteSubQueryRemoveRule toRule() {
            return new IgniteSubQueryRemoveRule(this);
        }

        /** Forwards a call to {@link #onMatch(RelOptRuleCall)}. */
        MatchHandler<IgniteSubQueryRemoveRule> matchHandler();

        /** Sets {@link #matchHandler()}. */
        Config withMatchHandler(MatchHandler<IgniteSubQueryRemoveRule> matchHandler);
    }
}

