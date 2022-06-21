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

package org.apache.ignite.internal.processors.query.calcite.exec;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.CollectNode;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.IndexSpoolNode;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.Node;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.ProjectNode;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.ScanNode;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.SortNode;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.apache.ignite.internal.processors.query.calcite.planner.TestTable;
import org.apache.ignite.internal.processors.query.calcite.prepare.BaseQueryContext;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexCount;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.query.calcite.util.RexUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.calcite.tools.Frameworks.createRootSchema;
import static org.apache.calcite.tools.Frameworks.newConfigBuilder;
import static org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor.FRAMEWORK_CONFIG;

/**
 * Test LogicalRelImplementor class.
 */
public class LogicalRelImplementorTest extends GridCommonAbstractTest {
    /** */
    private LogicalRelImplementor<Object[]> relImplementor;

    /** */
    private RelOptCluster cluster;

    /** */
    private ScanAwareTable tbl;

    /** */
    private BaseQueryContext qctx;

    /** */
    private RexBuilder rexBuilder;

    /** */
    private IgniteTypeFactory tf;

    /** */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        tf = Commons.typeFactory();
        RelDataTypeFactory.Builder b = new RelDataTypeFactory.Builder(tf);

        RelDataType sqlTypeInt = tf.createSqlType(SqlTypeName.INTEGER);
        RelDataType sqlTypeVarchar = tf.createSqlType(SqlTypeName.VARCHAR);

        b.add("_KEY", tf.createJavaType(Object.class));
        b.add("_VAL", tf.createJavaType(Object.class));
        b.add("ID", sqlTypeInt);
        b.add("VAL", sqlTypeVarchar);

        RelDataType rowType = b.build();

        tbl = new ScanAwareTable(rowType);

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");
        publicSchema.addTable("TBL", tbl);

        qctx = BaseQueryContext.builder()
            .frameworkConfig(
                newConfigBuilder(FRAMEWORK_CONFIG)
                    .defaultSchema(createRootSchema(false).add(publicSchema.getName(), publicSchema))
                    .build()
            )
            .logger(log)
            .build();

        UUID nodeId = UUID.randomUUID();

        ExecutionContext<Object[]> ectx = new ExecutionContext<Object[]>(
            qctx,
            null,
            null,
            nodeId,
            nodeId,
            null,
            null,
            ArrayRowHandler.INSTANCE,
            null
        ) {
            @Override public ColocationGroup group(long srcId) {
                return ColocationGroup.forNodes(Collections.singletonList(nodeId));
            }
        };

        relImplementor = new LogicalRelImplementor<>(
            ectx,
            null,
            null,
            null,
            null
        );

        cluster = Commons.emptyCluster();

        rexBuilder = cluster.getRexBuilder();
    }

    /**
     * Tests IndexCount execution plan is changed to Collect/Scan when index is unavailable.
     */
    @Test
    public void testIndexCountRewriter() {
        IgniteIndexCount idxCnt = new IgniteIndexCount(cluster, cluster.traitSet(),
            qctx.catalogReader().getTable(F.asList("PUBLIC", "TBL")), QueryUtils.PRIMARY_KEY_INDEX);

        checkCollectNode(relImplementor.visit(idxCnt));

        tbl.addIndex(QueryUtils.PRIMARY_KEY_INDEX, 2);

        Node<?> node = relImplementor.visit(idxCnt);

        assertTrue(node instanceof ScanNode);
        assertNull(node.sources());
        assertEquals(node.rowType(),
            tf.createStructType(F.asList(tf.createSqlType(SqlTypeName.BIGINT)), F.asList("COUNT")));

        tbl.markIndexRebuildInProgress(true);

        checkCollectNode(relImplementor.visit(idxCnt));
    }

    /** */
    private void checkCollectNode(Node<Object[]> node) {
        assertTrue(node instanceof CollectNode);
        assertTrue(node.sources() != null && node.sources().size() == 1);
        assertTrue(node.sources().get(0) instanceof ScanNode);
        assertNull(node.sources().get(0).sources());
        assertEquals(tbl.getRowType(tf), node.sources().get(0).rowType());
    }

    /** */
    @Test
    public void testIndexScanRewriter() {
        tbl.addIndex(QueryUtils.PRIMARY_KEY_INDEX, 2);

        RelDataType rowType = tbl.getRowType(tf);
        RelDataType sqlTypeInt = rowType.getFieldList().get(2).getType();
        RelDataType sqlTypeVarchar = rowType.getFieldList().get(3).getType();

        // Projects, filters and required columns.
        List<RexNode> project = F.asList(
            rexBuilder.makeLocalRef(sqlTypeVarchar, 1),
            rexBuilder.makeLocalRef(sqlTypeInt, 0),
            rexBuilder.makeCall(SqlStdOperatorTable.PLUS,
                rexBuilder.makeLocalRef(sqlTypeInt, 0),
                rexBuilder.makeLiteral(1, sqlTypeInt))
        );

        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
            rexBuilder.makeLocalRef(sqlTypeInt, 0),
            rexBuilder.makeLiteral(1, sqlTypeInt)
        );

        ImmutableBitSet requiredColumns = ImmutableBitSet.of(2, 3);

        // Collations.
        RelCollation idxCollation = tbl.getIndex(QueryUtils.PRIMARY_KEY_INDEX).collation();

        RelCollation colCollation = idxCollation.apply(Mappings.target(requiredColumns.asList(),
            tbl.getRowType(tf).getFieldCount()));

        RelCollation projCollation = TraitUtils.projectCollation(colCollation, RexUtils.replaceLocalRefs(project),
            tbl.getRowType(tf, requiredColumns));

        RelCollation emptyCollation = RelCollations.of();

        // Correlated projects and filters.
        RexShuttle replaceLiteralToCorr = new RexShuttle() {
            @Override public RexNode visitLiteral(RexLiteral literal) {
                return rexBuilder.makeFieldAccess(
                    rexBuilder.makeCorrel(tbl.getRowType(tf), new CorrelationId(0)), "ID", false);
            }
        };

        RexNode corrFilter = replaceLiteralToCorr.apply(filter);
        List<RexNode> corrProject = F.asList(project.get(0), project.get(1), replaceLiteralToCorr.apply(project.get(2)));

        tbl.markIndexRebuildInProgress(true);

        Predicate<Node<Object[]>> isScanNoFilterNoProject =
            node -> node instanceof ScanNode && !tbl.lastScanHasFilter && !tbl.lastScanHasProject;
        Predicate<Node<Object[]>> isScanWithFilterNoProject =
            node -> node instanceof ScanNode && tbl.lastScanHasFilter && !tbl.lastScanHasProject;
        Predicate<Node<Object[]>> isScanWithProjectNoFilter =
            node -> node instanceof ScanNode && !tbl.lastScanHasFilter && tbl.lastScanHasProject;
        Predicate<Node<Object[]>> isScanWithFilterWithProject =
            node -> node instanceof ScanNode && tbl.lastScanHasFilter && tbl.lastScanHasProject;

        Predicate<Node<Object[]>> isSort = node -> node instanceof SortNode;
        Predicate<Node<Object[]>> isSpool = node -> node instanceof IndexSpoolNode;
        Predicate<Node<Object[]>> isProj = node -> node instanceof ProjectNode;

        IgniteIndexScan templateScan = new IgniteIndexScan(
            cluster,
            cluster.traitSet(),
            qctx.catalogReader().getTable(F.asList("PUBLIC", "TBL")),
            "IDX",
            project,
            filter,
            RexUtils.buildSortedIndexConditions(cluster, idxCollation, filter, rowType, requiredColumns),
            requiredColumns,
            idxCollation
        );

        IgniteIndexScan scan;

        // IndexScan without filters and projects transforms to scan and sort.
        scan = createScan(templateScan, idxCollation, null, null, null);
        checkNodesChain(relImplementor, scan, isSort, isScanNoFilterNoProject);

        scan = createScan(templateScan, projCollation, null, null, requiredColumns);
        checkNodesChain(relImplementor, scan, isSort, isScanNoFilterNoProject);

        // IndexScan with simple filters and projects transforms to scan and sort.
        scan = createScan(templateScan, projCollation, project, filter, requiredColumns);
        checkNodesChain(relImplementor, scan, isSort, isScanWithFilterWithProject);

        scan = createScan(templateScan, colCollation, null, filter, requiredColumns);
        checkNodesChain(relImplementor, scan, isSort, isScanWithFilterNoProject);

        scan = createScan(templateScan, projCollation, project, null, requiredColumns);
        checkNodesChain(relImplementor, scan, isSort, isScanWithProjectNoFilter);

        // IndexScan with correlated filter without project transforms to scan, sort and spool.
        scan = createScan(templateScan, projCollation, null, corrFilter, requiredColumns);
        checkNodesChain(relImplementor, scan, isSpool, isSort, isScanNoFilterNoProject);

        // IndexScan with correlated filter without project transforms to scan, sort and spool.
        scan = createScan(templateScan, idxCollation, null, corrFilter, requiredColumns);
        checkNodesChain(relImplementor, scan, isSpool, isSort, isScanNoFilterNoProject);

        // IndexScan with correlated filter with project transforms to scan, sort, spool and project.
        scan = createScan(templateScan, projCollation, project, corrFilter, requiredColumns);
        checkNodesChain(relImplementor, scan, isProj, isSpool, isSort, isScanNoFilterNoProject);

        // IndexScan with correlated project transforms to scan, sort, spool and project.
        scan = createScan(templateScan, projCollation, corrProject, null, requiredColumns);
        checkNodesChain(relImplementor, scan, isProj, isSpool, isSort, isScanNoFilterNoProject);

        scan = createScan(templateScan, projCollation, corrProject, filter, requiredColumns);
        checkNodesChain(relImplementor, scan, isProj, isSpool, isSort, isScanWithFilterNoProject);

        scan = createScan(templateScan, projCollation, corrProject, corrFilter, requiredColumns);
        checkNodesChain(relImplementor, scan, isProj, isSpool, isSort, isScanNoFilterNoProject);

        // IndexScan with simple project without collation transforms to scan.
        List<RexNode> unknownCollationProject = new ArrayList<>(1);
        unknownCollationProject.add(corrProject.get(0)); // Field "val".

        scan = createScan(templateScan, emptyCollation, unknownCollationProject, filter, requiredColumns);
        checkNodesChain(relImplementor, scan, isScanWithFilterWithProject);

        scan = createScan(templateScan, emptyCollation, unknownCollationProject, null, requiredColumns);
        checkNodesChain(relImplementor, scan, isScanWithProjectNoFilter);

        // IndexScan with correlated project without collation transforms to scan, sort, spool and project.
        List<RexNode> unknownCollationCorrProject = new ArrayList<>(1);
        unknownCollationCorrProject.add(corrProject.get(2)); // Field "id + $cor0.id".

        scan = createScan(templateScan, emptyCollation, unknownCollationCorrProject, filter, requiredColumns);
        checkNodesChain(relImplementor, scan, isProj, isSpool, isSort, isScanWithFilterNoProject);

        scan = createScan(templateScan, emptyCollation, unknownCollationCorrProject, corrFilter, requiredColumns);
        checkNodesChain(relImplementor, scan, isProj, isSpool, isSort, isScanNoFilterNoProject);

        scan = createScan(templateScan, emptyCollation, unknownCollationCorrProject, null, requiredColumns);
        checkNodesChain(relImplementor, scan, isProj, isSpool, isSort, isScanNoFilterNoProject);
    }

    /** */
    private IgniteIndexScan createScan(
        IgniteIndexScan templateScan,
        RelCollation collation,
        List<RexNode> projects,
        RexNode filters,
        ImmutableBitSet requiredColumns
    ) {
        return new IgniteIndexScan(
            templateScan.getCluster(),
            templateScan.getTraitSet().replace(collation),
            templateScan.getTable(),
            templateScan.indexName(),
            projects,
            filters,
            templateScan.indexConditions(),
            requiredColumns,
            templateScan.collation()
        );
    }

    /** */
    private <Row> void checkNodesChain(
        LogicalRelImplementor<Row> relImplementor,
        IgniteIndexScan scan,
        Predicate<Node<Row>>... predicates
    ) {
        Node<Row> node = relImplementor.visit(scan);

        boolean lastFound = false;

        for (Predicate<Node<Row>> predicate : predicates) {
            assertFalse("Not enough nodes", lastFound);
            assertTrue("Node " + node + " doesn't match predicate", predicate.test(node));

            if (!F.isEmpty(node.sources()))
                node = node.sources().get(0);
            else
                lastFound = true;
        }

        assertTrue("Too much nodes", lastFound);
    }

    /** */
    private static class ScanAwareTable extends TestTable {
        /** */
        private volatile boolean lastScanHasFilter;

        /** */
        private volatile boolean lastScanHasProject;

        /** */
        public ScanAwareTable(RelDataType rowType) {
            super(rowType);
        }

        /** {@inheritDoc} */
        @Override public <Row> Iterable<Row> scan(
            ExecutionContext<Row> execCtx,
            ColocationGroup grp,
            Predicate<Row> filter,
            Function<Row, Row> transformer,
            ImmutableBitSet bitSet
        ) {
            lastScanHasFilter = filter != null;
            lastScanHasProject = transformer != null;
            return Collections.emptyList();
        }
    }
}
