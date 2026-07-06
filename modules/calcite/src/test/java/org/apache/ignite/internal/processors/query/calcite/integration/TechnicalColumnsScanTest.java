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

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import javax.cache.CacheException;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryImplEx;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
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
import org.apache.ignite.internal.processors.query.calcite.prepare.MultiStepQueryPlan;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.internal.processors.query.calcite.prepare.QueryPlan;
import org.apache.ignite.internal.processors.query.calcite.schema.ColumnDescriptor;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteCacheTable;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteIndex;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.transactions.IgniteTxTimeoutCheckedException;
import org.apache.ignite.testframework.GridTestUtils;
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
    @SuppressWarnings("unchecked")
    public void testScannedTechnicalColumnsCanLockTxEntries() throws Exception {
        createAndPopulatePersonTable();

        IgniteCacheTable tbl = personTable();
        ScanContext scanCtx = scanContext(tbl);
        List<Object[]> rows = materialize(tbl.scan(scanCtx.ectx, scanCtx.grp, lockRequiredColumns(tbl)));
        List<CacheEntry<Object, Object>> entries = new ArrayList<>();
        Integer expSrc = tbl.descriptor().cacheInfo().cacheId();

        assertEquals(30, rows.size());

        for (Object[] row : rows) {
            assertEquals(4, row.length);
            assertTrue("Unexpected _VER value [val=" + row[2] + ", cls=" +
                (row[2] == null ? null : row[2].getClass()) + ']', row[2] instanceof GridCacheVersion);
            assertEquals(expSrc, row[3]);

            entries.add(new CacheEntryImplEx<>(row[0], row[1], (GridCacheVersion)row[2]));
        }

        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            assertTrue(node.cache(tbl.descriptor().cacheInfo().name()).unwrap(IgniteCacheProxy.class)
                .internalProxy().lockTxEntries(entries, 5_000));

            checkInaccessInOtherTx();

            sql("UPDATE Person SET age = 42 WHERE id = 2");

            tx.commit();
        }

        List<List<?>> rowsAfterUpdate = sql("SELECT id, name, age FROM Person WHERE id = 2");

        assertEquals(1, rowsAfterUpdate.size());
        assertEquals(personName(2), rowsAfterUpdate.get(0).get(1));
        assertEquals(42, rowsAfterUpdate.get(0).get(2));
    }

    /**
     * Checks that another transaction cannot access the cache.
     *
     * @throws IgniteCheckedException If failed.
     */
    private void checkInaccessInOtherTx() throws IgniteCheckedException {
        IgniteInternalFuture<Void> accessFut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() {
                try (Transaction tx = node.transactions().txStart(PESSIMISTIC, READ_COMMITTED, 500, 1)) {
                    sql("UPDATE Person SET name = 'Charley' WHERE id = 2");

                    tx.commit();
                }

                return null;
            }
        });

        GridTestUtils.assertThrowsWithCause(new Callable<Object>() {
            @Override public Object call() throws Exception {
                return accessFut.get(10_000);
            }
        }, IgniteTxTimeoutCheckedException.class);
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

    /**
     * Verifies that internal Ignite code can plan a query referencing technical columns (_ver, _src)
     * by setting {@code allowTechnicalColumns(true)} on the planning context, even though the same
     * queries are rejected during regular user-facing SQL validation.
     */
    @Test
    public void testInternalQueryPlanningAllowsTechnicalColumns() throws Exception {
        createAndPopulatePersonTable();

        CalciteQueryProcessor qryProc = Commons.lookupComponent(node.context(), CalciteQueryProcessor.class);
        SchemaPlus schema = qryProc.schemaHolder().schema(QueryUtils.DFLT_SCHEMA);

        BaseQueryContext qctx = BaseQueryContext.builder()
            .logger(log)
            .defaultSchema(schema)
            .build();

        String sql = "SELECT id, _ver, _src FROM Person";

        PlanningContext pctx = PlanningContext.builder()
            .parentContext(qctx)
            .query(sql)
            .allowTechnicalColumns(true)
            .build();

        SqlNode sqlNode = pctx.planner().parse(new StringReader(sql));

        QueryPlan plan = qryProc.prepareService().prepareSingle(sqlNode, pctx);

        assertTrue("Expected MultiStepQueryPlan, got: " + plan.getClass(), plan instanceof MultiStepQueryPlan);

        List<String> fieldNames = ((MultiStepQueryPlan)plan).fieldsMetadata().rowType().getFieldNames();

        assertTrue("Plan must expose ID column", fieldNames.stream().anyMatch("ID"::equalsIgnoreCase));
        assertTrue("Plan must expose _VER column",
            fieldNames.stream().anyMatch(QueryUtils.VER_FIELD_NAME::equalsIgnoreCase));
        assertTrue("Plan must expose _SRC column",
            fieldNames.stream().anyMatch(QueryUtils.SRC_FIELD_NAME::equalsIgnoreCase));
    }

    /** */
    @Test
    public void testCannotCreateTableWithTechnicalColumnNames() {
        assertTechnicalColumnCreateForbidden("CREATE TABLE PersonVer (id INT PRIMARY KEY, _ver INT)",
            QueryUtils.VER_FIELD_NAME);
        assertTechnicalColumnCreateForbidden("CREATE TABLE PersonSrc (id INT PRIMARY KEY, _src INT)",
            QueryUtils.SRC_FIELD_NAME);
    }

    /** */
    @Test
    public void testCannotAddTechnicalColumnNames() throws Exception {
        createAndPopulatePersonTable();

        assertTechnicalColumnAddForbidden("ALTER TABLE Person ADD COLUMN _ver INT",
            QueryUtils.VER_FIELD_NAME);
        assertTechnicalColumnAddForbidden("ALTER TABLE Person ADD COLUMN _src INT",
            QueryUtils.SRC_FIELD_NAME);
    }

    /** */
    @Test
    public void testTechnicalColumnsAreHiddenFromSql() throws Exception {
        createAndPopulatePersonTable();

        List<List<?>> rows = sql("SELECT * FROM Person");

        assertEquals(30, rows.size());
        assertEquals(3, rows.get(0).size());

        assertTechnicalColumnAccessForbidden("SELECT _ver FROM Person");
        assertTechnicalColumnAccessForbidden("SELECT _src FROM Person");
        assertTechnicalColumnAccessForbidden("SELECT p._ver FROM Person p");
        assertTechnicalColumnAccessForbidden("SELECT id FROM Person WHERE _src IS NOT NULL");
        assertTechnicalColumnAccessForbidden("SELECT id FROM Person ORDER BY _ver");
        assertTechnicalColumnAccessForbidden("SELECT id FROM Person ORDER BY _ver DESC");
        assertTechnicalColumnAccessForbidden("SELECT id FROM Person ORDER BY _src NULLS FIRST");
        assertTechnicalColumnAccessForbidden("SELECT id FROM Person GROUP BY _ver");
        assertTechnicalColumnAccessForbidden("SELECT p1.id FROM Person p1 JOIN Person p2 ON p1._ver = p2._ver");
        assertTechnicalColumnAccessForbidden("SELECT CASE WHEN _ver IS NOT NULL THEN 1 ELSE 0 END FROM Person");
        assertTechnicalColumnAccessForbidden("SELECT CAST(_ver AS VARCHAR) FROM Person");
        assertTechnicalColumnAccessForbidden("SELECT id FROM Person WHERE (SELECT _ver FROM Person WHERE id = 1) IS NOT NULL");

        // MERGE: technical columns must not be exposed in all clause positions.
        assertTechnicalColumnAccessForbidden(
            "MERGE INTO Person " +
                "USING (SELECT id, _ver FROM Person) AS src ON (Person.id = src.id) " +
                "WHEN NOT MATCHED THEN INSERT (id, name, age) VALUES (src.id, 'x', 1)");

        assertTechnicalColumnAccessForbidden(
            "MERGE INTO Person " +
                "USING (SELECT 100 AS id) AS src ON (Person._ver IS NOT NULL AND Person.id = src.id) " +
                "WHEN NOT MATCHED THEN INSERT (id, name, age) VALUES (src.id, 'x', 1)");

        assertTechnicalColumnAccessForbidden(
            "MERGE INTO Person " +
                "USING (SELECT 1 AS id) AS src ON (Person.id = src.id) " +
                "WHEN MATCHED THEN UPDATE SET name = CAST(Person._ver AS VARCHAR)");
    }

    /** */
    private void assertTechnicalColumnCreateForbidden(String qry, String colName) {
        String expMsg = "Name '" + colName + "' is reserved and cannot be used as a field name";

        try {
            sql(qry);

            fail("Exception is expected");
        }
        catch (CacheException e) {
            assertTrue("Expected reserved field name exception was not found: " + e,
                hasCauseOrSuppressed(e, IgniteCheckedException.class, expMsg));
        }
    }

    /** */
    private boolean hasCauseOrSuppressed(Throwable t, Class<? extends Throwable> cls, String msg) {
        if (t == null)
            return false;

        if (cls.isAssignableFrom(t.getClass()) && t.getMessage() != null && t.getMessage().contains(msg))
            return true;

        if (hasCauseOrSuppressed(t.getCause(), cls, msg))
            return true;

        for (Throwable suppressed : t.getSuppressed()) {
            if (hasCauseOrSuppressed(suppressed, cls, msg))
                return true;
        }

        return false;
    }

    /** */
    private void assertTechnicalColumnAddForbidden(String qry, String colName) {
        GridTestUtils.assertThrowsAnyCause(log, () -> sql(qry), IgniteSQLException.class,
            "Column already exists: " + colName);
    }

    /** */
    private void assertTechnicalColumnAccessForbidden(String qry) {
        GridTestUtils.assertThrowsAnyCause(log, () -> sql(qry), IgniteSQLException.class, "Cannot access technical column");
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
            columnIndex(tbl, QueryUtils.VER_FIELD_NAME),
            columnIndex(tbl, QueryUtils.SRC_FIELD_NAME)
        );
    }

    /** */
    private ImmutableBitSet lockRequiredColumns(IgniteCacheTable tbl) {
        return ImmutableBitSet.of(
            columnIndex(tbl, QueryUtils.KEY_FIELD_NAME),
            columnIndex(tbl, QueryUtils.VAL_FIELD_NAME),
            columnIndex(tbl, QueryUtils.VER_FIELD_NAME),
            columnIndex(tbl, QueryUtils.SRC_FIELD_NAME)
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
