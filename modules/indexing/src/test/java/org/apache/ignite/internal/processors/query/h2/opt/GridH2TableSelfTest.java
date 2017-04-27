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
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.query.h2.database.H2PkHashIndex;
import org.apache.ignite.internal.processors.query.h2.database.H2RowFactory;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.h2.Driver;
import org.h2.index.Cursor;
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

    /** Hash. */
    private static final String HASH = "__GG_HASH";

    /** */
    private static final String STR_IDX_NAME = "__GG_IDX_";

    /** */
    private static final String NON_UNIQUE_IDX_NAME = "__GG_IDX_";

    /** */
    private static final String SCAN_IDX_NAME = GridH2PrimaryScanIndex.SCAN_INDEX_NAME_SUFFIX;

    /** */
    private Connection conn;

    /** */
    private GridH2Table tbl;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        // TODO: IGNITE-4994: Restore mock.
//        Driver.load();
//
//        conn = DriverManager.getConnection(DB_URL);
//
//        tbl = GridH2Table.Engine.createTable(conn, CREATE_TABLE_SQL, null, new GridH2Table.IndexesFactory() {
//            @Override public void onTableCreated(GridH2Table tbl) {
//                // No-op.
//            }
//
//            @Override public H2RowFactory createRowFactory(GridH2Table tbl) {
//                return null;
//            }
//
//            @Override public ArrayList<Index> createIndexes(GridH2Table tbl) {
//                ArrayList<Index> idxs = new ArrayList<>();
//
//                IndexColumn id = tbl.indexColumn(0, SortOrder.ASCENDING);
//                IndexColumn t = tbl.indexColumn(1, SortOrder.ASCENDING);
//                IndexColumn str = tbl.indexColumn(2, SortOrder.DESCENDING);
//                IndexColumn x = tbl.indexColumn(3, SortOrder.DESCENDING);
//
//                idxs.add(new H2PkHashIndex(null, tbl, HASH, F.asList(id)));
//                idxs.add(new GridH2TreeIndex(PK_NAME, tbl, true, F.asList(id)));
//                idxs.add(new GridH2TreeIndex(NON_UNIQUE_IDX_NAME, tbl, false, F.asList(x, t, id)));
//                idxs.add(new GridH2TreeIndex(STR_IDX_NAME, tbl, false, F.asList(str, id)));
//
//                return idxs;
//            }
//        }, null);
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
        return GridH2RowFactory.create(
            ValueUuid.get(id.getMostSignificantBits(), id.getLeastSignificantBits()),
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
    public void testIndexFindFirstOrLast() throws Exception {
        Index index = tbl.getIndexes().get(2);
        assertTrue(index instanceof GridH2TreeIndex);
        assertTrue(index.canGetFirstOrLast());

        //find first on empty data
        Cursor cursor = index.findFirstOrLast(null, true);
        assertFalse(cursor.next());
        assertNull(cursor.get());

        //find last on empty data
        cursor = index.findFirstOrLast(null, false);
        assertFalse(cursor.next());
        assertNull(cursor.get());

        //fill with data
        int rows = 100;
        long t = System.currentTimeMillis();
        Random rnd = new Random();
        UUID min = null;
        UUID max = null;

        for (int i = 0 ; i < rows; i++) {
            UUID id = UUID.randomUUID();
            if (min == null || id.compareTo(min) < 0)
                min = id;
            if (max == null || id.compareTo(max) > 0)
                max = id;
            GridH2Row row = row(id, t++, id.toString(), rnd.nextInt(100));
            ((GridH2TreeIndex)index).put(row);
        }

        //find first
        cursor = index.findFirstOrLast(null, true);
        assertTrue(cursor.next());
        assertEquals(min, cursor.get().getValue(0).getObject());
        assertFalse(cursor.next());

        //find last
        cursor = index.findFirstOrLast(null, false);
        assertTrue(cursor.next());
        assertEquals(max, cursor.get().getValue(0).getObject());
        assertFalse(cursor.next());
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
    private Set<Row> checkIndexesConsistent(ArrayList<Index> idxs, @Nullable Set<Row> rowSet) throws IgniteCheckedException {
        for (Index idx : idxs) {
            if (!(idx instanceof GridH2TreeIndex))
                continue;

            Set<Row> set = new HashSet<>();

            GridCursor<GridH2Row> cursor = ((GridH2TreeIndex)idx).rows();

            while(cursor.next())
                assertTrue(set.add(cursor.get()));

            //((GridH2SnapTreeSet)((GridH2Index)idx).tree).print();

            if (rowSet == null || rowSet.isEmpty())
                rowSet = set;
            else
                assertEquals(rowSet, set);
        }

        return rowSet;
    }

    /**
     * @param idxs Indexes list.
     */
    private void checkOrdered(ArrayList<Index> idxs) throws IgniteCheckedException {
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
    private void checkOrdered(GridH2TreeIndex idx, Comparator<? super GridH2Row> cmp) throws IgniteCheckedException {
        GridCursor<GridH2Row> cursor = idx.rows();

        GridH2Row min = null;

        while (cursor.next()) {
            GridH2Row row = cursor.get();

            System.out.println(row);

            assertNotNull(row);

            assertFalse("Incorrect row order in index: " + idx + "\n min: " + min + "\n row: " + row,
                min != null && cmp.compare(min, row) > 0);

            min = row;
        }
    }
}