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
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.metadata.CachingRelMetadataProvider;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.calcite.metadata.IgniteMetadata;
import org.apache.ignite.internal.processors.query.calcite.metadata.RelMetadataQueryEx;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteIndex;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;

import static org.apache.ignite.internal.processors.query.calcite.schema.IgniteTableImpl.PK_INDEX_NAME;

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
    private final SqlRexConvertletTable convertletTbl;

    /** */
    private final RexBuilder rexBuilder;

    /** */
    private final RexExecutor rexExecutor;

    /** */
    private final IgniteTypeFactory typeFactory;

    /** */
    private final SqlConformance conformance;

    /** */
    private final CalciteCatalogReader catalogReader;

    /** */
    private RelOptPlanner planner;

    /** */
    private SqlValidator validator;

    /**
     * @param ctx Planner context.
     */
    IgnitePlanner(PlanningContext ctx) {
        this.ctx = ctx;

        typeFactory = ctx.typeFactory();
        catalogReader = ctx.catalogReader();
        operatorTbl = ctx.opTable();
        conformance = ctx.conformance();
        frameworkCfg = ctx.config();

        programs = frameworkCfg.getPrograms();
        parserCfg = frameworkCfg.getParserConfig();
        sqlToRelConverterCfg = frameworkCfg.getSqlToRelConverterConfig();
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
        SqlToRelConverter sqlToRelConverter = new SqlToRelConverter(this,
            validator, catalogReader, createCluster(), convertletTbl, sqlToRelConverterCfg);

        RelRoot root = sqlToRelConverter.convertQuery(sql, false, true);
        root = root.withRel(sqlToRelConverter.decorrelate(sql, root.rel));

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
        SqlValidator validator = new IgniteSqlValidator(operatorTbl, catalogReader, typeFactory, conformance);
        validator.setIdentifierExpansion(true);

        SqlToRelConverter sqlToRelConverter = new SqlToRelConverter(this,
            validator, catalogReader, createCluster(), convertletTbl, sqlToRelConverterCfg);

        RelRoot root = sqlToRelConverter.convertQuery(sqlNode, true, false);
        root = root.withRel(sqlToRelConverter.decorrelate(sqlNode, root.rel));

        return root;
    }

    /** {@inheritDoc} */
    @Override public RelNode transform(int programIdx, RelTraitSet targetTraits, RelNode rel) {
        return programs.get(programIdx).run(planner(), rel, targetTraits.simplify(), materializations(rel), latices());
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
        return (T) phase.getProgram(ctx).run(planner(), rel, targetTraits.simplify(), materializations(rel), latices());
    }

    /** {@inheritDoc} */
    @Override public IgniteTypeFactory getTypeFactory() {
        return typeFactory;
    }

    /** */
    private RelOptPlanner planner() {
        if (planner == null) {
            planner = new VolcanoPlannerExt(frameworkCfg.getCostFactory(), ctx);
            planner.setExecutor(rexExecutor);

            for (RelTraitDef<?> def : traitDefs)
                planner.addRelTraitDef(def);
        }

        return planner;
    }

    /** */
    private SqlValidator validator() {
        if (validator == null)
            validator = new IgniteSqlValidator(operatorTbl, catalogReader, typeFactory, conformance);

        return validator;
    }

    /** Creates a cluster. */
    RelOptCluster createCluster() {
        RelOptCluster cluster = RelOptCluster.create(planner(), rexBuilder);

        cluster.setMetadataProvider(new CachingRelMetadataProvider(IgniteMetadata.METADATA_PROVIDER, planner()));
        cluster.setMetadataQuerySupplier(RelMetadataQueryEx::create);

        return cluster;
    }

    /** */
    private List<RelOptLattice> latices() {
        return ImmutableList.of(); // TODO
    }

    /**
     * Returns all applicable materializations (i.e. secondary indexes) for the given rel node,
     * @param root Rel node
     * @return Materializations.
     */
    private List<RelOptMaterialization> materializations(RelNode root) {
        RelOptCluster cluster = root.getCluster();

        List<RelOptTable> tbls = RelOptUtil.findAllTables(root);

        List<RelOptMaterialization> idxMaterializations = new ArrayList<>();

        for (RelOptTable tbl : tbls) {
            IgniteTable igniteTbl = tbl.unwrap(IgniteTable.class);

            IgniteTableScan tblScan = igniteTbl.toRel(cluster, tbl, PK_INDEX_NAME);

            for (IgniteIndex idx : igniteTbl.indexes().values()) {
                ImmutableList<String> names = ImmutableList.<String>builder()
                    .addAll(Util.skipLast(tbl.getQualifiedName()))
                    .add(Util.last(tbl.getQualifiedName()) + "[" + idx.name() + "]")
                    .build();

                IgniteTableScan idxTblScan = igniteTbl.toRel(cluster, tbl, idx.name());

                idxMaterializations.add(new RelOptMaterialization(idxTblScan, tblScan, null, names));
            }
        }

        return idxMaterializations;
    }

    /** */
    private static class VolcanoPlannerExt extends VolcanoPlanner {
        /** */
        private static final Boolean IMPATIENT = IgniteSystemProperties.getBoolean("IGNITE_CALCITE_PLANER_IMPATIENT", true);

        /** */
        private static final Boolean AMBITIOUS = IgniteSystemProperties.getBoolean("IGNITE_CALCITE_PLANER_AMBITIOUS", true);

        /** */
        protected VolcanoPlannerExt(RelOptCostFactory costFactory, Context externalCtx) {
            super(costFactory, externalCtx);

            impatient = IMPATIENT;
            ambitious = AMBITIOUS;
        }
    }
}
