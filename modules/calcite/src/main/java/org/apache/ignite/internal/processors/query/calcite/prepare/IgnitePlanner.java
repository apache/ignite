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

import com.google.common.collect.ImmutableList;
import java.io.Reader;
import java.util.List;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptLattice;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.metadata.CachingRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.calcite.metadata.IgniteMetadata;
import org.apache.ignite.internal.processors.query.calcite.metadata.RelMetadataQueryEx;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;

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
    private final RexBuilder rexBuilder;

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

        rexBuilder = new RexBuilder(typeFactory);
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
    }

    /** {@inheritDoc} */
    @Override public SqlNode parse(Reader reader) throws SqlParseException {
        SqlNodeList sqlNodes = SqlParser.create(reader, parserCfg).parseStmtList();

        return sqlNodes.size() == 1 ? sqlNodes.get(0) : sqlNodes;
    }

    /** {@inheritDoc} */
    @Override public SqlNode validate(SqlNode sqlNode) throws ValidationException {
        try {
            return validator().validate(sqlNode);
        }
        catch (RuntimeException e) {
            throw new ValidationException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public Pair<SqlNode, RelDataType> validateAndGetType(SqlNode sqlNode) {
        SqlNode validatedNode = validator().validate(sqlNode);
        RelDataType type = validator().getValidatedNodeType(validatedNode);
        return Pair.of(validatedNode, type);
    }

    /**
     * Validates a SQL statement.
     *
     * @param sqlNode Root node of the SQL parse tree.
     * @return Validated node, its validated type and type's origins.
     */
    public ValidationResult validateAndGetTypeMetadata(SqlNode sqlNode) {
        SqlNode validatedNode = validator().validate(sqlNode);
        RelDataType type = validator().getValidatedNodeType(validatedNode);
        List<List<String>> origins = validator().getFieldOrigins(validatedNode);

        return new ValidationResult(validatedNode, type, origins);
    }

    /** {@inheritDoc} */
    @Override public RelNode convert(SqlNode sql) {
        return rel(sql).project();
    }

    /** {@inheritDoc} */
    @Override public RelRoot rel(SqlNode sql) {
        SqlToRelConverter sqlToRelConverter = sqlToRelConverter(validator(), catalogReader, sqlToRelConverterCfg);
        RelRoot root = sqlToRelConverter.convertQuery(sql, false, true);
        root = root.withRel(sqlToRelConverter.decorrelate(sql, root.rel));
        root = root.withRel(root.project());
        root = trimUnusedFields(root);

        return root;
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
        SqlValidator validator = new IgniteSqlValidator(operatorTbl, catalogReader, typeFactory, validatorCfg);
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
        return (T) phase.getProgram(ctx).run(planner(), rel, targetTraits.simplify(), materializations(), latices());
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
        }

        return planner;
    }

    /** */
    private SqlValidator validator() {
        if (validator == null)
            validator = new IgniteSqlValidator(operatorTbl, catalogReader, typeFactory, validatorCfg);

        return validator;
    }

    /** Creates a cluster. */
    RelOptCluster cluster() {
        if (cluster == null) {
            cluster = RelOptCluster.create(planner(), rexBuilder);
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
    protected RelRoot trimUnusedFields(RelRoot root) {
        final SqlToRelConverter.Config config = SqlToRelConverter.configBuilder()
            .withConfig(sqlToRelConverterCfg)
            // For now, don't trim if there are more than 3 joins. The projects
            // near the leaves created by trim migrate past joins and seem to
            // prevent join-reordering.
            .withTrimUnusedFields(RelOptUtil.countJoins(root.rel) < 2)
            .build();
        SqlToRelConverter converter = sqlToRelConverter(validator(), catalogReader, config);
        boolean ordered = !root.collation.getFieldCollations().isEmpty();
        boolean dml = SqlKind.DML.contains(root.kind);
        return root.withRel(converter.trimUnusedFields(dml || ordered, root.rel));
    }

    /** */
    private SqlToRelConverter sqlToRelConverter(SqlValidator validator, CalciteCatalogReader reader,
        SqlToRelConverter.Config config) {
        return new SqlToRelConverter(this, validator, reader, cluster(), convertletTbl, config);
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
            if (rel instanceof IgniteRel && ((IgniteRel)rel).distribution().getType() == RelDistribution.Type.ANY)
                return getCostFactory().makeInfiniteCost(); // force certain distributions

            return super.getCost(rel, mq);
        }
    }
}
