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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
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
import org.apache.ignite.internal.processors.query.calcite.schema.ColumnDescriptor;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteCacheTable;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteIndex;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.junit.Test;

/** Tests system columns returned by direct table and index scans. */
public class SystemColumnsScanTest extends AbstractBasicIntegrationTest {
    /** {@inheritDoc} */
    @Override protected int nodeCount() {
        return 1;
    }

    /** */
    @Test
    public void testTableScanReturnsSystemColumns() throws Exception {
        createAndPopulatePersonTable();

        IgniteCacheTable tbl = personTable();
        ScanContext scanCtx = scanContext(tbl);

        assertSystemColumns(tbl.scan(scanCtx.ectx, scanCtx.grp, requiredSystemColumns(tbl)));
    }

    /** */
    @Test
    public void testIndexScanReturnsSystemColumns() throws Exception {
        createAndPopulatePersonTable();

        IgniteCacheTable tbl = personTable();
        IgniteIndex idx = tbl.getIndex("AGE_IDX");

        assertNotNull(idx);

        ScanContext scanCtx = scanContext(tbl);

        assertSystemColumns(idx.scan(scanCtx.ectx, scanCtx.grp, null, requiredSystemColumns(tbl)));
    }

    /** Verifies that an explicit SQL query returns system columns. */
    @Test
    public void testExplicitSelectReturnsSystemColumns() throws Exception {
        createAndPopulatePersonTable();

        assertQuery("SELECT _key, _val, _ver FROM Person")
            .columnNames("_KEY", "_VAL", "_VER")
            .resultSize(30)
            .withResultChecker(this::assertSqlSystemColumns)
            .check();
    }

    /** */
    @Test
    public void testCannotCreateTableWithSystemColumnName() {
        assertSystemColumnCreateForbidden("CREATE TABLE PersonVer (id INT PRIMARY KEY, _ver INT)",
            QueryUtils.VER_FIELD_NAME);
    }

    /** */
    @Test
    public void testCannotAddSystemColumnName() throws Exception {
        createAndPopulatePersonTable();

        assertSystemColumnAddForbidden("ALTER TABLE Person ADD COLUMN _ver INT",
            QueryUtils.VER_FIELD_NAME);
    }

    /** */
    @Test
    public void testSystemColumnsAreHiddenFromSelectStar() throws Exception {
        createAndPopulatePersonTable();

        assertQuery("SELECT * FROM Person")
            .columnNames("ID", "NAME", "AGE")
            .resultSize(30)
            .check();
    }

    /** */
    @Test
    public void testVersionColumnCannotBeDmlTarget() throws Exception {
        createAndPopulatePersonTable();

        assertVersionColumnDmlTargetForbidden(
            "INSERT INTO Person (id, name, age, _ver) VALUES (1, 'Ann', 21, NULL)");
        assertVersionColumnDmlTargetForbidden("UPDATE Person SET _ver = NULL");
    }

    /** */
    private void assertSystemColumnCreateForbidden(String qry, String colName) {
        String expMsg = "Name '" + colName + "' is reserved and cannot be used as a field name";

        try {
            sql(qry);

            fail("Exception is expected");
        }
        catch (Exception e) {
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
    private void assertSystemColumnAddForbidden(String qry, String colName) {
        assertThrows(qry, IgniteSQLException.class, "Column already exists: " + colName);
    }

    /** */
    private void assertVersionColumnDmlTargetForbidden(String qry) {
        assertThrows(qry, IgniteSQLException.class, "Cannot modify system column");
    }

    /** */
    private void createAndPopulatePersonTable() throws Exception {
        sql("CREATE TABLE Person (id INT PRIMARY KEY, name VARCHAR, age INT)");
        sql("CREATE INDEX age_idx ON Person(age)");

        for (int i = 1; i <= 30; i++)
            sql("INSERT INTO Person(id, name, age) VALUES (?, ?, ?)", i, personName(i), 20 + i);

        awaitPartitionMapExchange();
    }

    /** */
    private void assertSqlSystemColumns(List<List<?>> rows) {
        for (List<?> row : rows) {
            assertEquals(3, row.size());
            assertTrue(row.get(0) instanceof Integer);
            assertNotNull(row.get(1));
            assertTrue("Unexpected _VER value: " + row.get(2), row.get(2) instanceof GridCacheVersion);
        }
    }

    /** */
    private void assertSystemColumns(Iterable<Object[]> rowsIterable) throws Exception {
        List<Object[]> rows = materialize(rowsIterable);
        Set<Integer> ids = new HashSet<>();

        assertEquals(30, rows.size());

        for (Object[] row : rows) {
            assertEquals(3, row.length);
            assertTrue(row[0] instanceof Integer);
            assertNotNull(row[1]);
            assertTrue("Unexpected _VER value [val=" + row[2] + ", cls=" +
                (row[2] == null ? null : row[2].getClass()) + ']', row[2] instanceof GridCacheVersion);

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
    private ImmutableBitSet requiredSystemColumns(IgniteCacheTable tbl) {
        return ImmutableBitSet.of(
            columnIndex(tbl, QueryUtils.KEY_FIELD_NAME),
            columnIndex(tbl, QueryUtils.VAL_FIELD_NAME),
            columnIndex(tbl, QueryUtils.VER_FIELD_NAME)
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
        IgniteEx node = grid(0);
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
        IgniteEx node = grid(0);
        CalciteQueryProcessor qryProc = Commons.lookupComponent(node.context(), CalciteQueryProcessor.class);

        return (IgniteCacheTable)qryProc.schemaHolder().schema(QueryUtils.DFLT_SCHEMA).getTable("PERSON");
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
