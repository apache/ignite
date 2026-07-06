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

package org.apache.ignite.internal.processors.query.calcite.integration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor;
import org.apache.ignite.internal.processors.query.calcite.exec.ArrayRowHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.tracker.NoOpIoTracker;
import org.apache.ignite.internal.processors.query.calcite.exec.tracker.NoOpMemoryTracker;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.apache.ignite.internal.processors.query.calcite.metadata.FragmentDescription;
import org.apache.ignite.internal.processors.query.calcite.metadata.FragmentMapping;
import org.apache.ignite.internal.processors.query.calcite.prepare.BaseQueryContext;
import org.apache.ignite.internal.processors.query.calcite.prepare.MappingQueryContext;
import org.apache.ignite.internal.processors.query.calcite.schema.ColumnDescriptor;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteCacheTable;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteIndex;
import org.apache.ignite.internal.processors.query.calcite.schema.TechnicalColumns;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;

/** Tests technical columns returned by direct table and index scans. */
public class TechnicalColumnsScanTest extends GridCommonAbstractTest {
    /** */
    private IgniteEx node;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setSqlConfiguration(new SqlConfiguration().setQueryEnginesConfiguration(
                new CalciteQueryEngineConfiguration().setDefault(true)))
            .setTransactionConfiguration(new TransactionConfiguration().setTxAwareQueriesEnabled(true));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        node = startGrid(0);

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** */
    @Test
    public void testTableScanReturnsTechnicalColumns() throws Exception {
        createAndPopulatePersonTable();

        IgniteCacheTable tbl = personTable();
        ScanContext scanCtx = scanContext(tbl);

        assertTechnicalColumns(tbl.scan(scanCtx.ectx, scanCtx.grp, requiredColumns(tbl)), tbl);
    }

    /** */
    @Test
    public void testIndexScanReturnsTechnicalColumns() throws Exception {
        createAndPopulatePersonTable();

        IgniteCacheTable tbl = personTable();
        IgniteIndex idx = tbl.getIndex("AGE_IDX");

        assertNotNull(idx);

        ScanContext scanCtx = scanContext(tbl);

        assertTechnicalColumns(idx.scan(scanCtx.ectx, scanCtx.grp, null, requiredColumns(tbl)), tbl);
    }


    /** */
    private void createAndPopulatePersonTable() throws Exception {
        sql("CREATE TABLE Person (id INT PRIMARY KEY, name VARCHAR, age INT) WITH atomicity=TRANSACTIONAL");
        sql("CREATE INDEX age_idx ON Person(age)");

        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            for (int i = 1; i <= 30; i++)
                sql("INSERT INTO Person(id, name, age) VALUES (?, ?, ?)", i, personName(i), 20 + i);

            tx.commit();
        }

        awaitPartitionMapExchange();
    }

    /** */
    private void assertTechnicalColumns(Iterable<Object[]> rowsIterable, IgniteCacheTable tbl) throws Exception {
        List<Object[]> rows = materialize(rowsIterable);
        Set<Integer> ids = new HashSet<>();
        Integer expSrc = tbl.descriptor().cacheInfo().cacheId();

        assertEquals(30, rows.size());

        for (Object[] row : rows) {
            assertEquals(3, row.length);
            assertTrue(row[0] instanceof Integer);
            assertTrue("Unexpected _VER value [val=" + row[1] + ", cls=" +
                (row[1] == null ? null : row[1].getClass()) + ']', row[1] instanceof GridCacheVersion);
            assertEquals(expSrc, row[2]);

            ids.add((Integer)row[0]);
        }

        for (int i = 1; i <= 30; i++)
            assertTrue("Missing id: " + i, ids.contains(i));
    }

    /** */
    private List<Object[]> materialize(Iterable<Object[]> rowsIterable) throws Exception {
        List<Object[]> rows = new ArrayList<>();

        try {
            for (Object[] row : rowsIterable)
                rows.add(row);
        }
        finally {
            if (rowsIterable instanceof AutoCloseable)
                ((AutoCloseable)rowsIterable).close();
        }

        return rows;
    }

    /** */
    private ImmutableBitSet requiredColumns(IgniteCacheTable tbl) {
        return ImmutableBitSet.of(
            columnIndex(tbl, "ID"),
            columnIndex(tbl, TechnicalColumns.VER_FIELD_NAME),
            columnIndex(tbl, TechnicalColumns.SRC_FIELD_NAME)
        );
    }

    /** */
    private int columnIndex(IgniteCacheTable tbl, String name) {
        ColumnDescriptor desc = tbl.descriptor().columnDescriptor(name);

        assertNotNull(name, desc);

        return desc.fieldIndex();
    }

    /** */
    private ScanContext scanContext(IgniteCacheTable tbl) {
        UUID nodeId = node.localNode().id();
        AffinityTopologyVersion topVer = node.context().cache().context().exchange().readyAffinityVersion();
        BaseQueryContext qctx = BaseQueryContext.builder().logger(log).build();

        ExecutionContext<Object[]> ectx = new ExecutionContext<>(
            qctx,
            null,
            null,
            UUID.randomUUID(),
            nodeId,
            nodeId,
            topVer,
            new FragmentDescription(0, FragmentMapping.create(nodeId), null, Collections.emptyMap()),
            ArrayRowHandler.INSTANCE,
            NoOpMemoryTracker.INSTANCE,
            NoOpIoTracker.INSTANCE,
            0,
            Collections.emptyMap(),
            null
        );

        ColocationGroup grp = tbl.colocationGroup(new MappingQueryContext(qctx, nodeId, topVer, null)).finalizeMapping();

        return new ScanContext(ectx, grp);
    }

    /** */
    private IgniteCacheTable personTable() {
        CalciteQueryProcessor qryProc = Commons.lookupComponent(node.context(), CalciteQueryProcessor.class);

        return (IgniteCacheTable)qryProc.schemaHolder().schema(QueryUtils.DFLT_SCHEMA).getTable("PERSON");
    }

    /** */
    private List<List<?>> sql(String sql, Object... args) {
        return node.context().query().querySqlFields(new SqlFieldsQuery(sql).setSchema("PUBLIC").setArgs(args), true).getAll();
    }

    /** */
    private String personName(int id) {
        switch (id) {
            case 1:
                return "Alice";

            case 2:
                return "Bob";

            case 3:
                return "Ann";

            case 4:
                return "Carl";

            case 5:
                return "Alex";

            case 6:
                return "Diana";

            default:
                return "Person" + id;
        }
    }

    /** */
    private static class ScanContext {
        /** */
        private final ExecutionContext<Object[]> ectx;

        /** */
        private final ColocationGroup grp;

        /** */
        private ScanContext(ExecutionContext<Object[]> ectx, ColocationGroup grp) {
            this.ectx = ectx;
            this.grp = grp;
        }
    }
}
