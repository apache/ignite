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
import java.util.Properties;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.plan.volcano.VolcanoUtils;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.metadata.CachingRelMetadataProvider;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.processors.query.calcite.metadata.IgniteMetadata;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.serialize.Graph;
import org.apache.ignite.internal.processors.query.calcite.serialize.relation.GraphToRelConverter;
import org.apache.ignite.internal.processors.query.calcite.serialize.relation.RelGraph;
import org.apache.ignite.internal.processors.query.calcite.serialize.relation.RelToGraphConverter;
import org.apache.ignite.internal.processors.query.calcite.splitter.QueryPlan;
import org.apache.ignite.internal.processors.query.calcite.splitter.Splitter;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeSystem;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

/**
 *
 */
public class IgnitePlanner implements Planner, RelOptTable.ViewExpander {
    private final SqlOperatorTable operatorTable;
    private final ImmutableList<Program> programs;
    private final FrameworkConfig frameworkConfig;
    private final Context context;
    private final CalciteConnectionConfig connectionConfig;
    private final ImmutableList<RelTraitDef> traitDefs;
    private final SqlParser.Config parserConfig;
    private final SqlToRelConverter.Config sqlToRelConverterConfig;
    private final SqlRexConvertletTable convertletTable;
    private final RexExecutor executor;
    private final SchemaPlus defaultSchema;
    private final JavaTypeFactory typeFactory;

    private boolean open;

    private RelOptPlanner planner;
    private RelMetadataProvider metadataProvider;
    private SqlValidator validator;

    /**
     * @param config Framework config.
     */
    public IgnitePlanner(FrameworkConfig config) {
        frameworkConfig = config;
        defaultSchema = config.getDefaultSchema();
        operatorTable = config.getOperatorTable();
        programs = config.getPrograms();
        parserConfig = config.getParserConfig();
        sqlToRelConverterConfig = config.getSqlToRelConverterConfig();
        traitDefs = config.getTraitDefs();
        convertletTable = config.getConvertletTable();
        executor = config.getExecutor();
        context = config.getContext();
        connectionConfig = connConfig();

        RelDataTypeSystem typeSystem = connectionConfig
            .typeSystem(RelDataTypeSystem.class, IgniteTypeSystem.DEFAULT);

        typeFactory = new IgniteTypeFactory(typeSystem);

        Commons.plannerContext(context).planner(this);
    }

    private CalciteConnectionConfig connConfig() {
        CalciteConnectionConfig unwrapped = context.unwrap(CalciteConnectionConfig.class);
        if (unwrapped != null)
            return unwrapped;

        Properties properties = new Properties();
        properties.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(),
            String.valueOf(parserConfig.caseSensitive()));
        properties.setProperty(CalciteConnectionProperty.CONFORMANCE.camelName(),
            String.valueOf(frameworkConfig.getParserConfig().conformance()));
        return new CalciteConnectionConfigImpl(properties);
    }

    /** {@inheritDoc} */
    @Override public RelTraitSet getEmptyTraitSet() {
        return planner.emptyTraitSet();
    }

    /** {@inheritDoc} */
    @Override public void close() {
        reset();
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        planner = null;
        metadataProvider = null;
        validator = null;

        RelMetadataQuery.THREAD_PROVIDERS.remove();

        open = false;
    }

    private void ready() {
        if (!open) {
            planner = VolcanoUtils.impatient(new VolcanoPlanner(frameworkConfig.getCostFactory(), context));
            planner.setExecutor(executor);
            metadataProvider = new CachingRelMetadataProvider(IgniteMetadata.METADATA_PROVIDER, planner);

            validator = new IgniteSqlValidator(operatorTable(), createCatalogReader(), typeFactory, conformance());
            validator.setIdentifierExpansion(true);

            for (RelTraitDef def : traitDefs) {
                planner.addRelTraitDef(def);
            }

            open = true;
        }
    }

    /** {@inheritDoc} */
    @Override public SqlNode parse(Reader reader) throws SqlParseException {
        return SqlParser.create(reader, parserConfig).parseStmt();
    }

    /** {@inheritDoc} */
    @Override public SqlNode validate(SqlNode sqlNode) throws ValidationException {
        ready();

        try {
            return validator.validate(sqlNode);
        }
        catch (RuntimeException e) {
            throw new ValidationException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public Pair<SqlNode, RelDataType> validateAndGetType(SqlNode sqlNode) throws ValidationException {
        ready();

        SqlNode validatedNode = validate(sqlNode);
        RelDataType type = validator.getValidatedNodeType(validatedNode);
        return Pair.of(validatedNode, type);
    }

    /** {@inheritDoc} */
    @Override public RelNode convert(SqlNode sql) {
        return rel(sql).rel;
    }

    public RelNode convert(RelGraph graph) {
        ready();

        RelOptCluster cluster = createCluster(createRexBuilder());
        RelBuilder relBuilder = createRelBuilder(cluster, createCatalogReader());

        return new GraphToRelConverter(this, relBuilder, operatorTable).convert(graph);
    }

    /** {@inheritDoc} */
    @Override public RelRoot rel(SqlNode sql) {
        ready();

        RexBuilder rexBuilder = createRexBuilder();
        RelOptCluster cluster = createCluster(rexBuilder);
        SqlToRelConverter.Config config = SqlToRelConverter.configBuilder()
            .withConfig(sqlToRelConverterConfig)
            .withTrimUnusedFields(false)
            .withConvertTableAccess(false)
            .build();
        SqlToRelConverter sqlToRelConverter =
            new SqlToRelConverter(this, validator, createCatalogReader(), cluster, convertletTable, config);
        RelRoot root = sqlToRelConverter.convertQuery(sql, false, true);
        root = root.withRel(sqlToRelConverter.flattenTypes(root.rel, true));
        RelBuilder relBuilder = createRelBuilder(cluster, null);
        root = root.withRel(RelDecorrelator.decorrelateQuery(root.rel, relBuilder));
        return root;
    }

    public QueryPlan plan(RelNode rel) {
        ready();

        if (rel.getConvention() != IgniteConvention.INSTANCE)
            throw new IllegalArgumentException("Physical node is required.");

        return new Splitter().go((IgniteRel) rel);
    }

    public Graph graph(RelNode rel) {
        ready();

        if (rel.getConvention() != IgniteConvention.INSTANCE)
            throw new IllegalArgumentException("Physical node is required.");

        return new RelToGraphConverter().go((IgniteRel) rel);
    }

    /** {@inheritDoc} */
    @Override public RelRoot expandView(RelDataType rowType, String queryString, List<String> schemaPath, List<String> viewPath) {
        ready();

        SqlParser parser = SqlParser.create(queryString, parserConfig);
        SqlNode sqlNode;
        try {
            sqlNode = parser.parseQuery();
        }
        catch (SqlParseException e) {
            throw new RuntimeException("parse failed", e);
        }

        SqlConformance conformance = conformance();
        CalciteCatalogReader catalogReader =
            createCatalogReader().withSchemaPath(schemaPath);
        SqlValidator validator = new IgniteSqlValidator(operatorTable(), catalogReader, typeFactory, conformance);
        validator.setIdentifierExpansion(true);

        RexBuilder rexBuilder = createRexBuilder();
        RelOptCluster cluster = createCluster(rexBuilder);
        SqlToRelConverter.Config config = SqlToRelConverter
            .configBuilder()
            .withConfig(sqlToRelConverterConfig)
            .withTrimUnusedFields(false)
            .withConvertTableAccess(false)
            .build();
        SqlToRelConverter sqlToRelConverter =
            new SqlToRelConverter(this, validator,
                catalogReader, cluster, convertletTable, config);

        RelRoot root = sqlToRelConverter.convertQuery(sqlNode, true, false);
        RelRoot root2 = root.withRel(sqlToRelConverter.flattenTypes(root.rel, true));
        RelBuilder relBuilder = createRelBuilder(cluster, null);
        return root2.withRel(RelDecorrelator.decorrelateQuery(root.rel, relBuilder));
    }

    private RelOptCluster createCluster(RexBuilder rexBuilder) {
        RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);

        cluster.setMetadataProvider(metadataProvider);
        RelMetadataQuery.THREAD_PROVIDERS.set(JaninoRelMetadataProvider.of(metadataProvider));

        return cluster;
    }

    /** {@inheritDoc} */
    @Override public RelNode transform(int programIdx, RelTraitSet targetTraits, RelNode rel) {
        ready();

        RelTraitSet toTraits = targetTraits.simplify();

        rel.accept(new MetaDataProviderModifier(metadataProvider));

        return programs.get(programIdx).run(planner, rel, toTraits, ImmutableList.of(), ImmutableList.of());
    }

    public RelNode transform(PlannerType plannerType, PlannerPhase plannerPhase, RelNode input, RelTraitSet targetTraits)  {
        ready();

        RelTraitSet toTraits = targetTraits.simplify();

        input.accept(new MetaDataProviderModifier(metadataProvider));

        RelNode output;

        switch (plannerType) {
            case HEP:
                final HepProgramBuilder programBuilder = new HepProgramBuilder();

                for (RelOptRule rule : plannerPhase.getRules(Commons.plannerContext(context))) {
                    programBuilder.addRuleInstance(rule);
                }

                final HepPlanner hepPlanner =
                    new HepPlanner(programBuilder.build(), context, true, null, RelOptCostImpl.FACTORY);

                hepPlanner.setRoot(input);

                if (!input.getTraitSet().equals(targetTraits))
                    hepPlanner.changeTraits(input, toTraits);

                output = hepPlanner.findBestExp();

                break;
            case VOLCANO:
                Program program = Programs.of(plannerPhase.getRules(Commons.plannerContext(context)));

                output = program.run(planner, input, toTraits,
                    ImmutableList.of(), ImmutableList.of());

                break;
            default:
                throw new AssertionError("Unknown planner type: " + plannerType);
        }

        return output;
    }

    /** {@inheritDoc} */
    @Override public JavaTypeFactory getTypeFactory() {
        return typeFactory;
    }

    private SqlConformance conformance() {
        return connectionConfig.conformance();
    }

    private SqlOperatorTable operatorTable() {
        return operatorTable;
    }

    private RexBuilder createRexBuilder() {
        return new RexBuilder(typeFactory);
    }

    private RelBuilder createRelBuilder(RelOptCluster cluster, RelOptSchema schema) {
        return sqlToRelConverterConfig.getRelBuilderFactory().create(cluster, schema);
    }

    private CalciteCatalogReader createCatalogReader() {
        SchemaPlus rootSchema = rootSchema(defaultSchema);

        return new CalciteCatalogReader(
            CalciteSchema.from(rootSchema),
            CalciteSchema.from(defaultSchema).path(null),
            typeFactory, connectionConfig);
    }

    private static SchemaPlus rootSchema(SchemaPlus schema) {
        for (; ; ) {
            if (schema.getParentSchema() == null) {
                return schema;
            }
            schema = schema.getParentSchema();
        }
    }

    /** */
    private static class MetaDataProviderModifier extends RelShuttleImpl {
        /** */
        private final RelMetadataProvider metadataProvider;

        /** */
        private MetaDataProviderModifier(RelMetadataProvider metadataProvider) {
            this.metadataProvider = metadataProvider;
        }

        /** {@inheritDoc} */
        @Override public RelNode visit(TableScan scan) {
            scan.getCluster().setMetadataProvider(metadataProvider);
            return super.visit(scan);
        }

        /** {@inheritDoc} */
        @Override public RelNode visit(TableFunctionScan scan) {
            scan.getCluster().setMetadataProvider(metadataProvider);
            return super.visit(scan);
        }

        /** {@inheritDoc} */
        @Override public RelNode visit(LogicalValues values) {
            values.getCluster().setMetadataProvider(metadataProvider);
            return super.visit(values);
        }

        /** {@inheritDoc} */
        @Override protected RelNode visitChild(RelNode parent, int i, RelNode child) {
            child.accept(this);
            parent.getCluster().setMetadataProvider(metadataProvider);
            return parent;
        }
    }
}
