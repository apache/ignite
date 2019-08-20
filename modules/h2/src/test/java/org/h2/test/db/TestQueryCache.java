/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import org.h2.api.ErrorCode;
import org.h2.test.TestBase;

/**
 * Tests the query cache.
 */
public class TestQueryCache extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws Exception {
        deleteDb("queryCache");
        test1();
        testClearingCacheWithTableStructureChanges();
        deleteDb("queryCache");
    }

    private void test1() throws Exception {
        try (Connection conn = getConnection("queryCache;QUERY_CACHE_SIZE=10")) {
            Statement stat = conn.createStatement();
            stat.execute("create table test(id int, name varchar)");
            PreparedStatement prep;
            // query execution may be fast here but the parsing must be slow
            StringBuilder queryBuilder = new StringBuilder("select count(*) from test t1 where \n");
            for (int i = 0; i < 1000; i++) {
                if (i != 0) {
                    queryBuilder.append(" and ");
                }
                queryBuilder.append(" TIMESTAMP '2005-12-31 23:59:59' = TIMESTAMP '2005-12-31 23:59:59' ");
            }
            String query = queryBuilder.toString();
            conn.prepareStatement(query);
            int firstGreater = 0;
            int firstSmaller = 0;
            long time;
            ResultSet rs;
            long first = 0;
            // 1000 iterations to warm up and avoid JIT effects
            for (int i = 0; i < 1005; i++) {
                // this should both ensure results are not re-used
                // stat.execute("set mode regular");
                // stat.execute("create table x()");
                // stat.execute("drop table x");
                time = System.nanoTime();
                prep = conn.prepareStatement(query);
                execute(prep);
                prep.close();
                rs = stat.executeQuery(query);
                rs.next();
                int c = rs.getInt(1);
                rs.close();
                assertEquals(0, c);
                time = System.nanoTime() - time;
                if (i == 1000) {
                    // take from cache and do not close,
                    // so that next iteration will have a cache miss
                    prep = conn.prepareStatement(query);
                } else if (i == 1001) {
                    first = time;
                    // try to avoid pauses in subsequent iterations
                    System.gc();
                } else if (i > 1001) {
                    if (first > time) {
                        firstGreater++;
                    } else {
                        firstSmaller++;
                    }
                }
            }
            // first prepare time must be always greater because of query cache,
            // but JVM is too unpredictable to assert that, so just check that
            // usually this is true
            assertSmaller(firstSmaller, firstGreater);
            stat.execute("drop table test");
        }
    }

    private void testClearingCacheWithTableStructureChanges() throws Exception {
        try (Connection conn = getConnection("queryCache;QUERY_CACHE_SIZE=10")) {
            assertThrows(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, conn).
                    prepareStatement("SELECT * FROM TEST");
            Statement stat = conn.createStatement();
            stat.executeUpdate("CREATE TABLE TEST(col1 bigint, col2 varchar(255))");
            PreparedStatement prep = conn.prepareStatement("SELECT * FROM TEST");
            prep.close();
            stat.executeUpdate("DROP TABLE TEST");
            assertThrows(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, conn).
                    prepareStatement("SELECT * FROM TEST");
        }
    }
}
