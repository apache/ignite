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
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.RelOptTableImpl;
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
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.util.Pair;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.calcite.metadata.IgniteMetadata;
import org.apache.ignite.internal.processors.query.calcite.metadata.RelMetadataQueryEx;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;

/**
 * Query planer.
 */
public class IgnitePlanner implements Planner, RelOptTable.ViewExpander {
    /** */
    private final SqlOperatorTable operatorTable;

    /** */
    private final ImmutableList<Program> programs;

    /** */
    private final FrameworkConfig frameworkConfig;

    /** */
    private final PlanningContext ctx;

    /** */
    @SuppressWarnings("rawtypes")
    private final ImmutableList<RelTraitDef> traitDefs;

    /** */
    private final SqlParser.Config parserConfig;

    /** */
    private final SqlToRelConverter.Config sqlToRelConverterConfig;

    /** */
    private final SqlRexConvertletTable convertletTable;

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
        operatorTable = ctx.opTable();
        conformance = ctx.conformance();
        frameworkConfig = ctx.config();

        programs = frameworkConfig.getPrograms();
        parserConfig = frameworkConfig.getParserConfig();
        sqlToRelConverterConfig = frameworkConfig.getSqlToRelConverterConfig();
        convertletTable = frameworkConfig.getConvertletTable();
        rexExecutor = frameworkConfig.getExecutor();
        traitDefs = frameworkConfig.getTraitDefs();

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
        SqlNodeList sqlNodes = SqlParser.create(reader, parserConfig).parseStmtList();

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
    @Override public Pair<SqlNode, RelDataType> validateAndGetType(SqlNode sqlNode) throws ValidationException {
        SqlNode validatedNode = validator().validate(sqlNode);
        RelDataType type = validator().getValidatedNodeType(validatedNode);
        return Pair.of(validatedNode, type);
    }

    /**
     * Validates a SQL statement.
     *
     * @param sqlNode Root node of the SQL parse tree.
     * @return Validated node, its validated type and type's origins.
     * @throws ValidationException if not valid
     */
    public ValidationResult validateAndGetTypeMetadata(SqlNode sqlNode) throws ValidationException {
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
            validator, catalogReader, createCluster(), convertletTable, sqlToRelConverterConfig);

        return sqlToRelConverter.convertQuery(sql, false, true);
    }

    /** {@inheritDoc} */
    @Override public RelRoot expandView(RelDataType rowType, String queryString, List<String> schemaPath, List<String> viewPath) {
        SqlParser parser = SqlParser.create(queryString, parserConfig);
        SqlNode sqlNode;
        try {
            sqlNode = parser.parseQuery();
        }
        catch (SqlParseException e) {
            throw new IgniteSQLException("parse failed", IgniteQueryErrorCode.PARSING, e);
        }

        SqlConformance conformance = this.conformance;
        CalciteCatalogReader catalogReader = this.catalogReader.withSchemaPath(schemaPath);
        SqlValidator validator = new IgniteSqlValidator(operatorTable, catalogReader, typeFactory, conformance);
        validator.setIdentifierExpansion(true);

        SqlToRelConverter sqlToRelConverter = new SqlToRelConverter(this,
            validator, catalogReader, createCluster(), convertletTable, sqlToRelConverterConfig);

        return sqlToRelConverter.convertQuery(sqlNode, true, false);
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
    public <T extends RelNode> T transform(PlannerPhase phase, RelTraitSet targetTraits, RelNode rel)  {
        return (T) phase.getProgram(ctx).run(planner(), rel, targetTraits.simplify(), materializations(rel), latices());
    }

    /** {@inheritDoc} */
    @Override public IgniteTypeFactory getTypeFactory() {
        return typeFactory;
    }

    /** */
    private RelOptPlanner planner() {
        if (planner == null) {
            planner = new VolcanoPlannerExt(frameworkConfig.getCostFactory(), ctx);
            planner.setExecutor(rexExecutor);

            for (RelTraitDef<?> def : traitDefs)
                planner.addRelTraitDef(def);
        }

        return planner;
    }

    /** */
    private SqlValidator validator() {
        if (validator == null)
            validator = new IgniteSqlValidator(operatorTable, catalogReader, typeFactory, conformance);

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
    private RelBuilder createRelBuilder(RelOptCluster cluster, RelOptSchema schema) {
        return sqlToRelConverterConfig.getRelBuilderFactory().create(cluster, schema);
    }

    /** */
    private List<RelOptLattice> latices() {
        return ImmutableList.of(); // TODO
    }

    /** */
    private List<RelOptMaterialization> materializations(RelNode root) {
        RelOptCluster cluster = root.getCluster();

        List<RelOptTable> tbls = RelOptUtil.findAllTables(root);

        List<RelOptMaterialization> idxMaterializations = new ArrayList<>();

        for (RelOptTable tbl : tbls) {
            IgniteTable igniteTbl = tbl.unwrap(IgniteTable.class);

            IgniteTableScan tblScan = igniteTbl.toRel(cluster, tbl);

            for (IgniteTable idxTbl : igniteTbl.indexes().values()) {
                ImmutableList<String> names = ImmutableList.<String>builder()
                    .addAll(Util.skipLast(tbl.getQualifiedName()))
                    .add(idxTbl.name())
                    .build();

                RelOptTable idxRelTbl = RelOptTableImpl.create(tbl.getRelOptSchema(), tbl.getRowType(), idxTbl,  names);

                IgniteTableScan idxScan = idxTbl.toRel(cluster, idxRelTbl);

                idxMaterializations.add(new RelOptMaterialization(idxScan, tblScan, null, names));
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
        protected VolcanoPlannerExt(RelOptCostFactory costFactory, Context externalContext) {
            super(costFactory, externalContext);

            impatient = IMPATIENT;
            ambitious = AMBITIOUS;
        }
    }
}
