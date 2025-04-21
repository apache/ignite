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

import java.io.PrintWriter;
import java.io.Reader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptLattice;
import org.apache.calcite.plan.RelOptListener;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.metadata.CachingRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlNonNullableAccessors;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.RuleSets;
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.calcite.metadata.IgniteMdRowCount;
import org.apache.ignite.internal.processors.query.calcite.metadata.IgniteMetadata;
import org.apache.ignite.internal.processors.query.calcite.metadata.RelMetadataQueryEx;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Query planer.
 */
public class IgnitePlanner implements Planner, RelOptTable.ViewExpander {
    /** */
    private final SqlOperatorTable operatorTbl;

    /** */
    private final ImmutableList<Program> programs;

    /** */
    private final FrameworkConfig frameworkCfg;

    /** */
    private final PlanningContext ctx;

    /** */
    @SuppressWarnings("rawtypes")
    private final ImmutableList<RelTraitDef> traitDefs;

    /** */
    private final SqlParser.Config parserCfg;

    /** */
    private final SqlToRelConverter.Config sqlToRelConverterCfg;

    /** */
    private final SqlValidator.Config validatorCfg;

    /** */
    private final SqlRexConvertletTable convertletTbl;

    /** */
    private final RexExecutor rexExecutor;

    /** */
    private final IgniteTypeFactory typeFactory;

    /** */
    private final CalciteCatalogReader catalogReader;

    /** */
    private RelOptPlanner planner;

    /** */
    private SqlValidator validator;

    /** */
    private RelOptCluster cluster;

    /** */
    private @Nullable SqlNode validatedSqlNode;

    /**
     * TODO: Remove after IGNITE-18390.
     *
     * @see IgniteMdRowCount#joinRowCount(RelMetadataQuery, Join)
     */
    boolean heuristicJoinsOrder;

    /**
     * @param ctx Planner context.
     */
    IgnitePlanner(PlanningContext ctx) {
        this.ctx = ctx;

        typeFactory = ctx.typeFactory();
        catalogReader = ctx.catalogReader();
        operatorTbl = ctx.opTable();
        frameworkCfg = ctx.config();

        programs = frameworkCfg.getPrograms();
        parserCfg = frameworkCfg.getParserConfig();
        sqlToRelConverterCfg = frameworkCfg.getSqlToRelConverterConfig();
        validatorCfg = frameworkCfg.getSqlValidatorConfig();
        convertletTbl = frameworkCfg.getConvertletTable();
        rexExecutor = frameworkCfg.getExecutor();
        traitDefs = frameworkCfg.getTraitDefs();
    }

    /** {@inheritDoc} */
    @Override public RelTraitSet getEmptyTraitSet() {
        return planner().emptyTraitSet();
    }

    /** {@inheritDoc} */
    @Override public void close() {
        reset();
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        planner = null;
        validator = null;
        cluster = null;
        heuristicJoinsOrder = false;
    }

    /** {@inheritDoc} */
    @Override public SqlNode parse(Reader reader) throws SqlParseException {
        SqlNodeList sqlNodes = Commons.parse(reader, parserCfg);

        return sqlNodes.size() == 1 ? sqlNodes.get(0) : sqlNodes;
    }

    /** {@inheritDoc} */
    @Override public SqlNode validate(SqlNode sqlNode) throws ValidationException {
        try {
            validatedSqlNode = validator().validate(sqlNode);

            return validatedSqlNode;
        }
        catch (RuntimeException e) {
            throw new ValidationException(e.getMessage(), e);
        }
    }

    /** {@inheritDoc} */
    @Override public Pair<SqlNode, RelDataType> validateAndGetType(SqlNode sqlNode) {
        SqlNode validatedNode = validator().validate(sqlNode);
        RelDataType type = validator().getValidatedNodeType(validatedNode);
        return Pair.of(validatedNode, type);
    }

    /** {@inheritDoc} */
    @Override public RelDataType getParameterRowType() {
        return validator.getParameterRowType(validatedSqlNode);
    }

    /**
     * Converts a SQL data type specification to a relational data type.
     *
     * @param typeSpec Spec to convert from.
     * @return Relational type representation of given SQL type.
     */
    public RelDataType convert(SqlDataTypeSpec typeSpec) {
        return typeSpec.deriveType(validator());
    }

    /**
     * Validates a SQL statement.
     *
     * @param sqlNode Root node of the SQL parse tree.
     * @return Validated node, its validated type and type's origins.
     */
    public ValidationResult validateAndGetTypeMetadata(SqlNode sqlNode) {
        List<SqlNode> selectItems = null;
        List<SqlNode> selectItemsNoStar = null;
        SqlNode sqlNode0 = sqlNode instanceof SqlOrderBy ? ((SqlOrderBy)sqlNode).query : sqlNode;

        boolean hasStar = false;
        if (sqlNode0 instanceof SqlSelect) {
            selectItems = SqlNonNullableAccessors.getSelectList((SqlSelect)sqlNode0);
            selectItemsNoStar = new ArrayList<>(selectItems.size());

            for (SqlNode node : selectItems) {
                if (node instanceof SqlIdentifier) {
                    if (((SqlIdentifier)node).isStar())
                        hasStar = true;
                }
                else {
                    // We should track all non-identifiers for further processing if any star operator presents.
                    selectItemsNoStar.add(node);
                }
            }
        }

        SqlNode validatedNode = validator().validate(sqlNode);
        RelDataType type = validator().getValidatedNodeType(validatedNode);
        List<List<String>> origins = validator().getFieldOrigins(validatedNode);

        //    There is four cases.
        //    1. Simple column (SqlIdentifier) -- alias will be obtained from origins.
        //    2. Expanded column from star -- alias will be obtained from origins.
        //    3. AS call -- alias will be obtained from original call (second operand).
        //    4. Other call -- alias will be obtained via SqlValidator#deriveAlias.

        List<String> derived = null;
        if (validatedNode instanceof SqlSelect && !F.isEmpty(selectItems) && !F.isEmpty(selectItemsNoStar)) {
            derived = new ArrayList<>(selectItems.size());

            if (hasStar) {
                // If original SqlSelectNode has star, we should process expanded items.
                SqlNodeList expandedItems = ((SqlSelect)validatedNode).getSelectList();

                int cnt = 0;
                for (SqlNode node : expandedItems) {
                    // If the node is SqlIdentifier, alias will be obtained from origins.
                    if (node instanceof SqlIdentifier) {
                        derived.add(null);

                        continue;
                    }

                    if (node instanceof SqlBasicCall) {
                        if (cnt < selectItemsNoStar.size()) {
                            SqlNode noStarItem = selectItemsNoStar.get(cnt);

                            // The validator can transform SqlIdentifier to AS call. We should check whether
                            // AS call is a real one and take the second operand from original one.
                            if (isAsCall(noStarItem) && isAsCall(node)) {
                                SqlBasicCall origItem = (SqlBasicCall)noStarItem;
                                SqlBasicCall expandedItem = (SqlBasicCall)node;

                                if (Objects.equals(origItem.getParserPosition(), expandedItem.getParserPosition())) {
                                    derived.add(((SqlIdentifier)origItem.operand(1)).getSimple());
                                    cnt++;

                                    continue;
                                }
                            }
                            else {
                                // Other calls should be processed via SqlValidator#deriveAlias
                                derived.add(validator().deriveAlias(noStarItem, cnt++));

                                continue;
                            }
                        }
                        derived.add(null);

                        continue;
                    }

                    if (cnt < selectItemsNoStar.size())
                        derived.add(validator().deriveAlias(selectItemsNoStar.get(cnt), cnt++));
                    else
                        derived.add(null);
                }
            }
            else {
                int cnt = 0;
                for (SqlNode node : selectItems)
                    derived.add(validator().deriveAlias(node, cnt++));
            }
        }

        return new ValidationResult(validatedNode, type, origins, derived);
    }

    /** */
    private static boolean isAsCall(SqlNode node) {
        return node instanceof SqlBasicCall && node.getKind() == SqlKind.AS
                && ((SqlBasicCall)node).operandCount() == 2
                && ((SqlBasicCall)node).operand(0) instanceof SqlIdentifier
                && ((SqlBasicCall)node).operand(1) instanceof SqlIdentifier;
    }

    /** */
    public boolean heuristicJoinsOrder() {
        return heuristicJoinsOrder;
    }

    /** {@inheritDoc} */
    @Override public RelNode convert(SqlNode sql) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public RelRoot rel(SqlNode sql) {
        SqlToRelConverter sqlToRelConverter = sqlToRelConverter(validator(), catalogReader, sqlToRelConverterCfg);

        return sqlToRelConverter.convertQuery(sql, false, true);
    }

    /** {@inheritDoc} */
    @Override public RelRoot expandView(
        RelDataType rowType,
        String qryStr,
        List<String> schemaPath,
        List<String> viewPath
    ) {
        SqlParser parser = SqlParser.create(qryStr, parserCfg);
        SqlNode sqlNode;
        try {
            sqlNode = parser.parseQuery();
        }
        catch (SqlParseException e) {
            throw new IgniteSQLException("parse failed", IgniteQueryErrorCode.PARSING, e);
        }

        CalciteCatalogReader catalogReader = this.catalogReader.withSchemaPath(schemaPath);
        SqlValidator validator = new IgniteSqlValidator(operatorTbl, catalogReader, typeFactory, validatorCfg, ctx.parameters());
        SqlToRelConverter sqlToRelConverter = sqlToRelConverter(validator, catalogReader, sqlToRelConverterCfg);
        RelRoot root = sqlToRelConverter.convertQuery(sqlNode, true, false);
        root = root.withRel(sqlToRelConverter.decorrelate(sqlNode, root.rel));

        return root;
    }

    /** {@inheritDoc} */
    @Override public RelNode transform(int programIdx, RelTraitSet targetTraits, RelNode rel) {
        return programs.get(programIdx).run(planner(), rel, targetTraits.simplify(), materializations(), latices());
    }

    /**
     * Converts one relational nodes tree into another relational nodes tree
     * based on a particular planner type, planning phase and required set of traits.
     * @param phase Planner phase.
     * @param targetTraits Target traits.
     * @param rel Root node of relational tree.
     * @return The root of the new RelNode tree.
     */
    public <T extends RelNode> T transform(PlannerPhase phase, RelTraitSet targetTraits, RelNode rel) {
        return (T)phase.getProgram(ctx).run(planner(), rel, targetTraits.simplify(), materializations(), latices());
    }

    /** {@inheritDoc} */
    @Override public IgniteTypeFactory getTypeFactory() {
        return typeFactory;
    }

    /** */
    private RelOptPlanner planner() {
        if (planner == null) {
            VolcanoPlannerExt planner = new VolcanoPlannerExt(frameworkCfg.getCostFactory(), ctx);
            planner.setExecutor(rexExecutor);
            this.planner = planner;

            for (RelTraitDef<?> def : traitDefs)
                this.planner.addRelTraitDef(def);

            RelOptListener planLsnr = ctx.unwrap(RelOptListener.class);

            if (planLsnr != null)
                planner.addListener(planLsnr);
        }

        return planner;
    }

    /** */
    public String dump() {
        StringWriter w = new StringWriter();

        ((VolcanoPlanner)planner).dump(new PrintWriter(w));

        return w.toString();
    }

    /** */
    private SqlValidator validator() {
        if (validator == null)
            validator = new IgniteSqlValidator(operatorTbl, catalogReader, typeFactory, validatorCfg, ctx.parameters());

        return validator;
    }

    /** Creates a cluster. */
    RelOptCluster cluster() {
        if (cluster == null) {
            cluster = RelOptCluster.create(planner(), ctx.rexBuilder());
            cluster.setMetadataProvider(new CachingRelMetadataProvider(IgniteMetadata.METADATA_PROVIDER, planner()));
            cluster.setMetadataQuerySupplier(RelMetadataQueryEx::create);
        }

        return cluster;
    }

    /** */
    private List<RelOptLattice> latices() {
        return ImmutableList.of(); // TODO
    }

    /**
     * Returns all applicable materializations (i.e. secondary indexes) for the given rel node,
     * @return Materializations.
     */
    private List<RelOptMaterialization> materializations() {
        return ImmutableList.of(); // TODO
    }

    /**
     * Walks over a tree of relational expressions, replacing each
     * {@link org.apache.calcite.rel.RelNode} with a 'slimmed down' relational
     * expression that projects
     * only the columns required by its consumer.
     *
     * @param root Root of relational expression tree
     * @return Trimmed relational expression
     */
    public RelRoot trimUnusedFields(RelRoot root) {
        final SqlToRelConverter.Config cfg = sqlToRelConverterCfg
            .withExpand(false)
            .withTrimUnusedFields(true);
        SqlToRelConverter converter = sqlToRelConverter(validator(), catalogReader, cfg);
        boolean ordered = !root.collation.getFieldCollations().isEmpty();
        boolean dml = SqlKind.DML.contains(root.kind);
        return root.withRel(converter.trimUnusedFields(dml || ordered, root.rel));
    }

    /**
     * When rewriting sub-queries to {@code LogicalCorrelate} instances, correlate nodes with the same correlation ids
     * can be created (if there was more then one sub-query using the same correlate table). It's not a problem, when
     * rows are processed one by one (like in the enumerable convension), but Ignite execution nodes process batches
     * of rows, and execution nodes in some cases can get unexpected values for correlated variables.
     *
     * This method replaces collisions by variables in correlates. For the left hand of LogicalCorrelate duplicated
     * correlated variable and it's usages replaced with the new one. For example:
     *
     * LogicalCorrelate(correlation=[$cor0])                       LogicalCorrelate(correlation=[$cor0])
     *   LogicalCorrelate(correlation=[$cor0])    transforms to      LogicalCorrelate(correlation=[$cor1])
     *     ... condition=[=($cor0.A, $0)] ...                          ... condition=[=($cor1.A, $0)] ...
     *   ... condition=[=($cor0.A, $0)] ...                          ... condition=[=($cor0.A, $0)] ...
     *
     * For the right hand of LogicalCorrelate duplicated LogicalCorrelate is just replaced with regular join.
     * For example:
     *
     * LogicalCorrelate(correlation=[$cor0])                       LogicalCorrelate(correlation=[$cor0])
     *   ...                                      transforms to      ...
     *   LogicalCorrelate(correlation=[$cor0])                       LogicalJoin(condition=true)
     *
     * @param rel Relational expression tree.
     * @return Relational expression without collisions in correlates.
     */
    public RelNode replaceCorrelatesCollisions(RelNode rel) {
        RelShuttle relShuttle = new RelHomogeneousShuttle() {
            /** Set of used correlates. */
            private final Set<CorrelationId> usedSet = new HashSet<>();

            /** Map to find correlates, that should be replaced (in the left hand of correlate). */
            private final Map<CorrelationId, CorrelationId> replaceMap = new HashMap<>();

            /** Multiset to find correlates, that should be removed (in the right hand of correlate). */
            private final Map<CorrelationId, Integer> removeMap = new HashMap<>();

            /** */
            private final RexShuttle rexShuttle = new RexShuttle() {
                @Override public RexNode visitCorrelVariable(RexCorrelVariable variable) {
                    CorrelationId newCorId = replaceMap.get(variable.id);

                    if (newCorId != null)
                        return cluster().getRexBuilder().makeCorrel(variable.getType(), newCorId);
                    else
                        return variable;
                }
            };

            /** {@inheritDoc} */
            @Override public RelNode visit(LogicalCorrelate correlate) {
                CorrelationId corId = correlate.getCorrelationId();

                if (usedSet.contains(corId)) {
                    if (removeMap.containsKey(corId)) {
                        // We are in the right hand of correlate by corId: replace correlate with join.
                        RelNode join = LogicalJoin.create(
                            correlate.getLeft(),
                            correlate.getRight(),
                            Collections.emptyList(),
                            cluster().getRexBuilder().makeLiteral(true),
                            Collections.emptySet(),
                            correlate.getJoinType()
                        );

                        return super.visit(join);
                    }
                    else {
                        // We are in the right hand of correlate by corId: replace correlate variable.
                        CorrelationId newCorId = cluster().createCorrel();
                        CorrelationId oldCorId = replaceMap.put(corId, newCorId);

                        try {
                            correlate = correlate.copy(
                                correlate.getTraitSet(),
                                correlate.getLeft(),
                                correlate.getRight(),
                                newCorId,
                                correlate.getRequiredColumns(),
                                correlate.getJoinType()
                            );

                            return visitLeftAndRightCorrelateHands(correlate, corId);
                        }
                        finally {
                            if (oldCorId == null)
                                replaceMap.remove(corId);
                            else
                                replaceMap.put(corId, oldCorId);
                        }
                    }
                }
                else {
                    usedSet.add(corId);

                    return visitLeftAndRightCorrelateHands(correlate, corId);
                }
            }

            /** */
            private RelNode visitLeftAndRightCorrelateHands(LogicalCorrelate correlate, CorrelationId corId) {
                RelNode node = correlate;

                node = visitChild(node, 0, correlate.getLeft());

                removeMap.compute(corId, (k, v) -> v == null ? 1 : v + 1);

                try {
                    node = visitChild(node, 1, correlate.getRight());
                }
                finally {
                    removeMap.compute(corId, (k, v) -> v == 1 ? null : v - 1);
                }

                return node;
            }

            /** {@inheritDoc} */
            @Override public RelNode visit(RelNode other) {
                RelNode next = super.visit(other);

                return replaceMap.isEmpty() ? next : next.accept(rexShuttle);
            }
        };

        return relShuttle.visit(rel);
    }

    /**
     * Due to distributive property of conjunction over disjunction we can extract common part of conjunctions.
     * This can help us to simplify and push down filters.
     * For example, condition:
     *      (a = 0 and x = 1) or (a = 0 and y = 2) or (a = 0 and z = 3)
     * can be translated to:
     *      a = 0 and (x = 1 or y = 2 or z = 3)
     * after such a transformation condition "a = 0" can be used as index access predicate.
     */
    public RelNode extractConjunctionOverDisjunctionCommonPart(RelNode rel) {
        return new RelHomogeneousShuttle() {
            /** {@inheritDoc} */
            @Override public RelNode visit(LogicalFilter filter) {
                RexNode condition = transform(filter.getCluster().getRexBuilder(), filter.getCondition());

                if (condition != filter.getCondition())
                    filter = filter.copy(filter.getTraitSet(), filter.getInput(), condition);

                return super.visit(filter);
            }

            /** {@inheritDoc} */
            @Override public RelNode visit(LogicalJoin join) {
                RexNode condition = transform(join.getCluster().getRexBuilder(), join.getCondition());

                if (condition != join.getCondition()) {
                    join = join.copy(join.getTraitSet(), condition, join.getLeft(), join.getRight(),
                        join.getJoinType(), join.isSemiJoinDone());
                }

                return super.visit(join);
            }

            /** */
            private RexNode transform(RexBuilder rexBuilder, RexNode condition) {
                // It makes sence to extract only top level disjunction common part.
                if (!condition.isA(SqlKind.OR))
                    return condition;

                Set<RexNode> commonPart = new HashSet<>();

                List<RexNode> orOps = ((RexCall)condition).getOperands();

                RexNode firstOp = orOps.get(0);

                for (RexNode andOpFirst : conjunctionOperands(firstOp)) {
                    boolean found = false;

                    for (int i = 1; i < orOps.size(); i++) {
                        found = false;

                        for (RexNode andOpOther : conjunctionOperands(orOps.get(i))) {
                            if (andOpFirst.equals(andOpOther)) {
                                found = true;

                                break;
                            }
                        }

                        if (!found)
                            break;
                    }

                    if (found)
                        commonPart.add(andOpFirst);
                }

                if (commonPart.isEmpty())
                    return condition;

                List<RexNode> newOrOps = new ArrayList<>(orOps.size());

                for (RexNode orOp : orOps) {
                    List<RexNode> newAndOps = new ArrayList<>();

                    for (RexNode andOp : conjunctionOperands(orOp)) {
                        if (!commonPart.contains(andOp))
                            newAndOps.add(andOp);
                    }

                    if (!newAndOps.isEmpty()) {
                        RexNode newAnd = RexUtil.composeConjunction(rexBuilder, newAndOps);

                        newOrOps.add(newAnd);
                    }
                    else {
                        newOrOps.clear();

                        break;
                    }
                }

                if (newOrOps.isEmpty())
                    condition = RexUtil.composeConjunction(rexBuilder, commonPart);
                else {
                    RexNode newOr = RexUtil.composeDisjunction(rexBuilder, newOrOps);

                    List<RexNode> newConditions = new ArrayList<>(commonPart.size() + 1);
                    newConditions.addAll(commonPart);
                    newConditions.add(newOr);

                    condition = RexUtil.composeConjunction(rexBuilder, newConditions);
                }

                return condition;
            }

            /** */
            private List<RexNode> conjunctionOperands(RexNode call) {
                if (call.isA(SqlKind.AND))
                    return ((RexCall)call).getOperands();
                else
                    return Collections.singletonList(call);
            }
        }.visit(rel);
    }

    /** */
    private SqlToRelConverter sqlToRelConverter(SqlValidator validator, CalciteCatalogReader reader,
        SqlToRelConverter.Config config) {
        return new IgniteSqlToRelConvertor(this, validator, reader, cluster(), convertletTbl, config);
    }

    /** */
    public void addDisabledRules(Collection<String> disabledRuleNames) {
        if (F.isEmpty(disabledRuleNames))
            return;

        ctx.addRulesFilter(rulesSet -> {
            List<RelOptRule> newSet = new ArrayList<>();

            for (RelOptRule r : rulesSet) {
                if (!disabledRuleNames.contains(shortRuleName(r.toString())))
                    newSet.add(r);
            }

            return RuleSets.ofList(newSet);
        });
    }

    /** */
    private static String shortRuleName(String ruleDesc) {
        int pos = ruleDesc.indexOf('(');

        if (pos == -1)
            return ruleDesc;

        return ruleDesc.substring(0, pos);
    }

    /** */
    private static class VolcanoPlannerExt extends VolcanoPlanner {
        /** */
        protected VolcanoPlannerExt(RelOptCostFactory costFactory, Context externalCtx) {
            super(costFactory, externalCtx);
            setTopDownOpt(true);
        }

        /** {@inheritDoc} */
        @Override public RelOptCost getCost(RelNode rel, RelMetadataQuery mq) {
            return mq.getCumulativeCost(rel);
        }

        /** {@inheritDoc} */
        @Override public void checkCancel() {
            PlanningContext ctx = getContext().unwrap(PlanningContext.class);

            long timeout = ctx.plannerTimeout();

            if (timeout > 0) {
                long startTs = ctx.startTs();

                if (U.currentTimeMillis() - startTs > timeout)
                    cancelFlag.set(true);
            }

            super.checkCancel();
        }
    }
}
