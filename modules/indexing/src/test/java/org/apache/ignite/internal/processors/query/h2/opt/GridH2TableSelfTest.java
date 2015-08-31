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

package org.apache.ignite.internal.processors.query.h2.opt;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.h2.Driver;
import org.h2.index.Index;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.IndexColumn;
import org.h2.value.ValueLong;
import org.h2.value.ValueString;
import org.h2.value.ValueTimestamp;
import org.h2.value.ValueUuid;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;

/**
 * Tests H2 Table.
 */
@SuppressWarnings({"TypeMayBeWeakened", "FieldAccessedSynchronizedAndUnsynchronized"})
public class GridH2TableSelfTest extends GridCommonAbstractTest {
    /** */
    private static final long MAX_X = 2000;

    /** */
    private static final String DB_URL = "jdbc:h2:mem:gg_table_engine;MULTI_THREADED=1;OPTIMIZE_REUSE_RESULTS=0;" +
        "QUERY_CACHE_SIZE=0;RECOMPILE_ALWAYS=1";

    /** */
    private static final String CREATE_TABLE_SQL = "CREATE TABLE T(ID UUID, T TIMESTAMP, STR VARCHAR, X BIGINT)";

    /** */
    private static final String PK_NAME = "__GG_PK_";

    /** */
    private static final String STR_IDX_NAME = "__GG_IDX_";

    /** */
    private static final String NON_UNIQUE_IDX_NAME = "__GG_IDX_";

    /** */
    private static final String SCAN_IDX_NAME = GridH2Table.ScanIndex.SCAN_INDEX_NAME_SUFFIX;

    /** */
    private Connection conn;

    /** */
    private GridH2Table tbl;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        Driver.load();

        conn = DriverManager.getConnection(DB_URL);

        tbl = GridH2Table.Engine.createTable(conn, CREATE_TABLE_SQL, null, new GridH2Table.IndexesFactory() {
            @Override public ArrayList<Index> createIndexes(GridH2Table tbl) {
                ArrayList<Index> idxs = new ArrayList<>();

                IndexColumn id = tbl.indexColumn(0, SortOrder.ASCENDING);
                IndexColumn t = tbl.indexColumn(1, SortOrder.ASCENDING);
                IndexColumn str = tbl.indexColumn(2, SortOrder.DESCENDING);
                IndexColumn x = tbl.indexColumn(3, SortOrder.DESCENDING);

                idxs.add(new GridH2TreeIndex(PK_NAME, tbl, true, 0, 1, id));
                idxs.add(new GridH2TreeIndex(NON_UNIQUE_IDX_NAME, tbl, false, 0, 1, x, t));
                idxs.add(new GridH2TreeIndex(STR_IDX_NAME, tbl, false, 0, 1, str));

                return idxs;
            }
        }, null);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        conn.close();

        conn = null;
        tbl = null;
    }

    /**
     * @param id Id.
     * @param t Timestamp.
     * @param str String.
     * @param x X.
     * @return New row.
     */
    private GridH2Row row(UUID id, long t, String str, long x) {
        return new GridH2Row(ValueUuid.get(id.getMostSignificantBits(), id.getLeastSignificantBits()),
            ValueTimestamp.get(new Timestamp(t)),
            ValueString.get(str),
            ValueLong.get(x));
    }


    /**
     * Simple table test.
     *
     * @throws Exception If failed.
     */
    public void testTable() throws Exception {
        // Test insert.
        long x = MAX_X;

        Random rnd = new Random();

        while(x-- > 0) {
            UUID id = UUID.randomUUID();

            GridH2Row row = row(id, System.currentTimeMillis(), rnd.nextBoolean() ? id.toString() :
                UUID.randomUUID().toString(), rnd.nextInt(100));

            tbl.doUpdate(row, false);
        }

        assertEquals(MAX_X, tbl.getRowCountApproximation());
        assertEquals(MAX_X, tbl.getRowCount(null));

        for (GridH2IndexBase idx : tbl.indexes()) {
            assertEquals(MAX_X, idx.getRowCountApproximation());
            assertEquals(MAX_X, idx.getRowCount(null));
        }

        // Check correct rows order.
        checkOrdered((GridH2TreeIndex)tbl.indexes().get(0), new Comparator<SearchRow>() {
            @Override public int compare(SearchRow o1, SearchRow o2) {
                UUID id1 = (UUID)o1.getValue(0).getObject();
                UUID id2 = (UUID)o2.getValue(0).getObject();

                return id1.compareTo(id2);
            }
        });

        checkOrdered((GridH2TreeIndex)tbl.indexes().get(1), new Comparator<SearchRow>() {
            @Override public int compare(SearchRow o1, SearchRow o2) {
                Long x1 = (Long)o1.getValue(3).getObject();
                Long x2 = (Long)o2.getValue(3).getObject();

                int c = x2.compareTo(x1);

                if (c != 0)
                    return c;

                Timestamp t1 = (Timestamp)o1.getValue(1).getObject();
                Timestamp t2 = (Timestamp)o2.getValue(1).getObject();

                return t1.compareTo(t2);
            }
        });

        checkOrdered((GridH2TreeIndex)tbl.indexes().get(2), new Comparator<SearchRow>() {
            @Override public int compare(SearchRow o1, SearchRow o2) {
                String s1 = (String)o1.getValue(2).getObject();
                String s2 = (String)o2.getValue(2).getObject();

                return s2.compareTo(s1);
            }
        });

        // Indexes data consistency.
        ArrayList<? extends Index> idxs = tbl.indexes();

        checkIndexesConsistent((ArrayList<Index>)idxs, null);

        // Check unique index.
        UUID id = UUID.randomUUID();
        UUID id2 = UUID.randomUUID();

        assertTrue(tbl.doUpdate(row(id, System.currentTimeMillis(), id.toString(), rnd.nextInt(100)), false));
        assertTrue(tbl.doUpdate(row(id2, System.currentTimeMillis(), id2.toString(), rnd.nextInt(100)), false));

        // Check index selection.
        checkQueryPlan(conn, "SELECT * FROM T", SCAN_IDX_NAME);

        checkQueryPlan(conn, "SELECT * FROM T WHERE ID IS NULL", PK_NAME);
        checkQueryPlan(conn, "SELECT * FROM T WHERE ID = RANDOM_UUID()", PK_NAME);
        checkQueryPlan(conn, "SELECT * FROM T WHERE ID > RANDOM_UUID()", PK_NAME);
        checkQueryPlan(conn, "SELECT * FROM T ORDER BY ID", PK_NAME);

        checkQueryPlan(conn, "SELECT * FROM T WHERE STR IS NULL", STR_IDX_NAME);
        checkQueryPlan(conn, "SELECT * FROM T WHERE STR = 'aaaa'", STR_IDX_NAME);
        checkQueryPlan(conn, "SELECT * FROM T WHERE STR > 'aaaa'", STR_IDX_NAME);
        checkQueryPlan(conn, "SELECT * FROM T ORDER BY STR DESC", STR_IDX_NAME);

        checkQueryPlan(conn, "SELECT * FROM T WHERE X IS NULL", NON_UNIQUE_IDX_NAME);
        checkQueryPlan(conn, "SELECT * FROM T WHERE X = 10000", NON_UNIQUE_IDX_NAME);
        checkQueryPlan(conn, "SELECT * FROM T WHERE X > 10000", NON_UNIQUE_IDX_NAME);
        checkQueryPlan(conn, "SELECT * FROM T ORDER BY X DESC", NON_UNIQUE_IDX_NAME);
        checkQueryPlan(conn, "SELECT * FROM T ORDER BY X DESC, T", NON_UNIQUE_IDX_NAME);

        checkQueryPlan(conn, "SELECT * FROM T ORDER BY T, X DESC", SCAN_IDX_NAME);

        // Simple queries.

        Statement s = conn.createStatement();

        ResultSet rs = s.executeQuery("select id from t where x between 0 and 100");

        int i = 0;
        while (rs.next())
            i++;

        assertEquals(MAX_X + 2, i);

        // -----

        rs = s.executeQuery("select id from t where t is not null");

        i = 0;
        while (rs.next())
            i++;

        assertEquals(MAX_X + 2, i);

        // ----

        int cnt = 10 + rnd.nextInt(25);

        long t = System.currentTimeMillis();

        for (i = 0; i < cnt; i++) {
            id = UUID.randomUUID();

            assertTrue(tbl.doUpdate(row(id, t, id.toString(), 51), false));
        }

        rs = s.executeQuery("select x, id from t where x = 51 limit " + cnt);

        i = 0;

        while (rs.next()) {
            assertEquals(51, rs.getInt(1));

            i++;
        }

        assertEquals(cnt, i);
    }

    /**
     * Dumps all table rows for index.
     *
     * @param idx Index.
     */
    private void dumpRows(GridH2TreeIndex idx) {
        Iterator<GridH2Row> iter = idx.rows();

        while (iter.hasNext())
            System.out.println(iter.next().toString());
    }

    /**
     * Multithreaded indexes consistency test.
     *
     * @throws Exception If failed.
     */
    public void testIndexesMultiThreadedConsistency() throws Exception {
        final int threads = 19;
        final int iterations = 1500;

        multithreaded(new Callable<Void>() {
            @Override public Void call() throws Exception {
                Random rnd = new Random();

                PreparedStatement ps1 = null;

                for (int i = 0; i < iterations; i++) {
                    UUID id = UUID.randomUUID();

                    int x = rnd.nextInt(50);

                    long t = System.currentTimeMillis();

                    GridH2Row row = row(id, t, rnd.nextBoolean() ? id.toString() : UUID.randomUUID().toString(), x);

                    assertTrue(tbl.doUpdate(row, false));

                    if (rnd.nextInt(100) == 0) {
                        tbl.lock(null, false, false);

                        long cnt = 0;

                        try {
                            ArrayList<Index> idxs = tbl.getIndexes();

                            // Consistency check.
                            Set<Row> rowSet = checkIndexesConsistent(idxs, null);

                            // Order check.
                            checkOrdered(idxs);

                            checkIndexesConsistent(idxs, rowSet);

                            cnt = idxs.get(0).getRowCount(null);
                        }
                        finally {
                            tbl.unlock(null);
                        }

                        // Row count is valid.
                        ResultSet rs = conn.createStatement().executeQuery("select count(*) from t");

                        assertTrue(rs.next());

                        int cnt2 = rs.getInt(1);

                        rs.close();

                        assertTrue(cnt2 + " must be >= " + cnt, cnt2 >= cnt);
                        assertTrue(cnt2 <= threads * iterations);

                        // Search by ID.
                        rs = conn.createStatement().executeQuery("select * from t where id = '" + id.toString() + "'");

                        assertTrue(rs.next());
                        assertFalse(rs.next());

                        rs.close();

                        // Scan search.
                        if (ps1 == null)
                            ps1 = conn.prepareStatement("select id from t where x = ? order by t desc");

                        ps1.setInt(1, x);

                        rs = ps1.executeQuery();

                        for (;;) {
                            assertTrue(rs.next());

                            if (rs.getObject(1).equals(id))
                                break;
                        }

                        rs.close();
                    }
                }
                return null;
            }
        }, threads);
    }

    /**
     * Run test in endless loop.
     *
     * @param args Arguments.
     * @throws Exception If failed.
     */
    @SuppressWarnings("InfiniteLoopStatement")
    public static void main(String ... args) throws Exception {
        for (int i = 0;;) {
            GridH2TableSelfTest t = new GridH2TableSelfTest();

            t.beforeTest();

            t.testDataLoss();

            t.afterTest();

            System.out.println("..." + ++i);
        }
    }

    /**
      * @throws Exception If failed.
     */
    public void testRangeQuery() throws Exception {
        int rows = 3000;
        int xs = 37;

        long t = System.currentTimeMillis();

        Random rnd = new Random();

        for (int i = 0 ; i < rows; i++) {
            UUID id = UUID.randomUUID();

            GridH2Row row = row(id, t++, id.toString(), rnd.nextInt(xs));

            assertTrue(tbl.doUpdate(row, false));
        }

        PreparedStatement ps = conn.prepareStatement("select count(*) from t where x = ?");

        int cnt = 0;

        for (int x = 0; x < xs; x++) {
            ps.setInt(1, x);

            ResultSet rs = ps.executeQuery();

            assertTrue(rs.next());

            cnt += rs.getInt(1);
        }

        assertEquals(rows, cnt);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDataLoss() throws Exception {
        final int threads = 37;
        final int iterations = 15000;

        final AtomicInteger cntr = new AtomicInteger();

        final UUID[] ids = new UUID[threads * iterations];

        for (int i = 0; i < ids.length; i++)
            ids[i] = UUID.randomUUID();

        final long t = System.currentTimeMillis();

        final AtomicInteger deleted = new AtomicInteger();

        multithreaded(new Callable<Void>() {
            @Override public Void call() throws Exception {
                Random rnd = new Random();

                int offset = cntr.getAndIncrement() * iterations;

                synchronized (ids[offset]) {
                    for (int i = 0; i < iterations; i++) {
                        UUID id = ids[offset + i];

                        int x = rnd.nextInt(50);

                        GridH2Row row = row(id, t, id.toString(), x);

                        assertTrue(tbl.doUpdate(row, false));
                    }
                }

                offset = (offset + iterations) % ids.length;

                synchronized (ids[offset]) {
                    for (int i = 0; i < iterations; i += 2) {
                        UUID id = ids[offset + i];

                        int x = rnd.nextInt(50);

                        GridH2Row row = row(id, t, id.toString(), x);

                        if (tbl.doUpdate(row, true))
                            deleted.incrementAndGet();
                    }
                }

                return null;
            }
        }, threads);

        assertTrue(deleted.get() > 0);

        PreparedStatement p = conn.prepareStatement("select count(*) from t where id = ?");

        for (int i = 1; i < ids.length; i += 2) {
            p.setObject(1, ids[i]);

            ResultSet rs = p.executeQuery();

            assertTrue(rs.next());

            assertEquals(1, rs.getInt(1));
        }

        Statement s = conn.createStatement();

        ResultSet rs = s.executeQuery("select count(*) from t");

        assertTrue(rs.next());

        assertEquals(ids.length - deleted.get(), rs.getInt(1));
    }

    /**
     * @throws Exception If failed.
     */
    public void testRebuildIndexes() throws Exception {
        ArrayList<GridH2IndexBase> idxsBefore = tbl.indexes();

        assertEquals(3, idxsBefore.size());

        Random rnd = new Random();

        for (int i = 0; i < MAX_X; i++) {
            UUID id = UUID.randomUUID();

            GridH2Row row = row(id, System.currentTimeMillis(), rnd.nextBoolean() ? id.toString() :
                    UUID.randomUUID().toString(), rnd.nextInt(100));

            tbl.doUpdate(row, false);
        }

        for (GridH2IndexBase idx : idxsBefore)
            assertEquals(MAX_X, idx.getRowCountApproximation());

        tbl.rebuildIndexes();

        ArrayList<GridH2IndexBase> idxsAfter = tbl.indexes();

        assertEquals(3, idxsAfter.size());

        for (int i = 0; i < 3; i++) {
            GridH2IndexBase idxBefore = idxsBefore.get(i);
            GridH2IndexBase idxAfter = idxsAfter.get(i);

            assertNotSame(idxBefore, idxAfter);
            assertEquals(idxBefore.getName(), idxAfter.getName());
            assertSame(idxBefore.getTable(), idxAfter.getTable());
            assertEquals(idxBefore.getRowCountApproximation(), idxAfter.getRowCountApproximation());
            assertEquals(idxBefore.getIndexType().isUnique(), idxAfter.getIndexType().isUnique());
            Assert.assertArrayEquals(idxBefore.getColumns(), idxAfter.getColumns());
        }
    }

    /**
     * Check query plan to correctly select index.
     *
     * @param conn Connection.
     * @param sql Select.
     * @param search Search token in result.
     * @throws SQLException If failed.
     */
    private void checkQueryPlan(Connection conn, String sql, String search) throws SQLException {

        try (Statement s = conn.createStatement()) {
            try (ResultSet r = s.executeQuery("EXPLAIN ANALYZE " + sql)) {
                assertTrue(r.next());

                String plan = r.getString(1);

                assertTrue("Execution plan for '" + sql + "' query should contain '" + search + "'",
                        plan.contains(search));
            }
        }
    }

    /**
     * @param idxs Indexes.
     * @param rowSet Rows.
     * @return Rows.
     */
    private Set<Row> checkIndexesConsistent(ArrayList<Index> idxs, @Nullable Set<Row> rowSet) {
        for (Index idx : idxs) {
            if (!(idx instanceof GridH2TreeIndex))
                continue;

            Set<Row> set = new HashSet<>();

            Iterator<GridH2Row> iter = ((GridH2TreeIndex)idx).rows();

            while(iter.hasNext())
                assertTrue(set.add(iter.next()));

            //((GridH2SnapTreeSet)((GridH2Index)idx).tree).print();

            if (rowSet == null)
                rowSet = set;
            else
                assertEquals(rowSet, set);
        }

        return rowSet;
    }

    /**
     * @param idxs Indexes list.
     */
    private void checkOrdered(ArrayList<Index> idxs) {
        for (Index idx : idxs) {
            if (!(idx instanceof GridH2TreeIndex))
                continue;

            GridH2TreeIndex h2Idx = (GridH2TreeIndex)idx;

            checkOrdered(h2Idx, h2Idx);
        }
    }

    /**
     * @param idx Index.
     * @param cmp Comparator.
     */
    private void checkOrdered(GridH2TreeIndex idx, Comparator<? super GridH2Row> cmp) {
        Iterator<GridH2Row> rows = idx.rows();

        GridH2Row min = null;

        while (rows.hasNext()) {
            GridH2Row row = rows.next();

            assertNotNull(row);

            assertFalse("Incorrect row order in index: " + idx + "\n min: " + min + "\n row: " + row,
                min != null && cmp.compare(min, row) > 0);

            min = row;
        }
    }
}