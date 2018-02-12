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

package org.apache.ignite.jdbc.thin;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.BatchUpdateException;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Collections;
import java.util.Formattable;
import java.util.Formatter;
import java.util.Objects;
import java.util.concurrent.Callable;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.testframework.GridTestUtils;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_SQL_PARSER_DISABLE_H2_FALLBACK;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.util.IgniteUtils.resolveIgnitePath;

/**
 * COPY statement tests.
 */
public class JdbcThinBulkLoadPerfAbstractSelfTest extends JdbcThinAbstractDmlStatementSelfTest {
    /** JDBC statement. */
    private Statement stmt;

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfig() {
        return cacheConfigWithIndexedTypes();
    }

    /**
     * Creates cache configuration with {@link QueryEntity} created
     * using {@link CacheConfiguration#setIndexedTypes(Class[])} call.
     *
     * @return The cache configuration.
     */
    @SuppressWarnings("unchecked")
    private CacheConfiguration cacheConfigWithIndexedTypes() {
        CacheConfiguration<?,?> cache = defaultCacheConfiguration();

        cache.setCacheMode(cacheMode());
        cache.setAtomicityMode(atomicityMode());
        cache.setWriteSynchronizationMode(FULL_SYNC);

        if (cacheMode() == PARTITIONED)
            cache.setBackups(1);

        if (nearCache())
            cache.setNearConfiguration(new NearCacheConfiguration());

        return cache;
    }

    /**
     * Returns true if we are testing near cache.
     *
     * @return true if we are testing near cache.
     */
    protected boolean nearCache() { return false; };

    /**
     * Returns cache atomicity mode we are testing.
     *
     * @return The cache atomicity mode we are testing.
     */
    protected CacheAtomicityMode atomicityMode() { return CacheAtomicityMode.ATOMIC; };

    /**
     * Returns cache mode we are testing.
     *
     * @return The cache mode we are testing.
     */
    protected CacheMode cacheMode() { return CacheMode.PARTITIONED; }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        System.setProperty(IGNITE_SQL_PARSER_DISABLE_H2_FALLBACK, "TRUE");

        stmt = conn.createStatement();

        assertNotNull(stmt);
        assertFalse(stmt.isClosed());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        if (stmt != null && !stmt.isClosed())
            stmt.close();

        assertTrue(stmt.isClosed());

        System.clearProperty(IGNITE_SQL_PARSER_DISABLE_H2_FALLBACK);

        super.afterTest();
    }

    private void generateCsv(String fileName, int lineNum, int fldNum, int fldSize) throws FileNotFoundException {
        String lineNumFmt = "%" + String.format("%d", lineNum).length() + "s";
        String fldFmt = "%0" + fldSize + "d";

        if (Files.exists(Paths.get(fileName)))
            return;

        try (PrintWriter pw = new PrintWriter(new FileOutputStream(fileName))) {
            StringBuilder sb = new StringBuilder(fldNum * (fldSize + 3));

            for (int ln = 0; ln < lineNum; ln++) {
                sb.setLength(0);
                sb.append(String.format(lineNumFmt, ln));
                for (int f = 1; f < fldNum; f++) {
                    sb.append(',')
                      .append(String.format(fldFmt, f));
                }
                pw.println(sb.toString());
            }
        }
    }

    public void testPerf() throws SQLException, FileNotFoundException, InterruptedException {
        int lineCnt = 200_000;
        int tableFldCnt = 500;
        int fileFldCnt = 500;
        int fldSize = 10;
        boolean isPostgres = true;

        if (isPostgres) {
            conn = DriverManager.getConnection("jdbc:postgresql://localhost/postgres", "postgres", "admin");
            stmt = conn.createStatement();
        }

        System.out.print("\n\n\n\n\n");

        String fileName = "d:/tmp/testfile_" + lineCnt + "_" + fileFldCnt + "_" + fldSize + ".csv";

        System.out.println("Generating test file");
        generateCsv(fileName, lineCnt, fileFldCnt, fldSize);

        StringBuilder fieldsCreate = new StringBuilder();
        fieldsCreate.append("f000000 VARCHAR PRIMARY KEY");
        for (int i = 1; i < tableFldCnt; ++i)
            fieldsCreate.append(",").append(String.format("f%06d VARCHAR", i));

        System.out.println("Creating table");
        stmt.executeUpdate("drop table if exists public.FldTest2");
        stmt.executeUpdate("create table public.FldTest2 (" + fieldsCreate.toString() + ")");

        StringBuilder fieldsInsert = new StringBuilder();
        fieldsInsert.append("f000000");
        for (int i = 1; i < tableFldCnt; ++i)
            fieldsInsert.append(",").append(String.format("f%06d", i));

        for (int i = 0; i < 10; ++i) {
            long startNs = System.nanoTime();

            System.out.println("Running COPY");
            String sql = isPostgres
                ? ("copy public.FldTest2 " + "(" + fieldsInsert.toString() + ") from '" + fileName + "' with (format csv)")
                : ("copy from \"" + fileName + "\" into public.FldTest2 " + "(" + fieldsInsert.toString() + ") format csv");

            long lines = stmt.executeUpdate(sql);
            long stopNs = System.nanoTime();

            System.out.println("Checking records count");
            long recCnt;
            int tries = 0;
            do {
                Thread.sleep(100);
                ResultSet rs = stmt.executeQuery("select count(*) from public.FldTest2");
                rs.next();
                recCnt = rs.getInt(1);
                System.out.printf("Imported entries into the cache: %d\n", recCnt);
                tries++;
            }
            while (recCnt < lines && tries < 10);

            System.out.print(">>>>>>>>>>>\n" +
                "    Elapsed: " + String.format("%12.6f sec", (stopNs - startNs) / 10e9) + "\n" +
                "    Lines: " + lines + " => " + recCnt + "\n" +
                "    Lines/sec: " + String.format("%12.6f sec", 10e9 * ((double)lines / (stopNs - startNs))) + "\n");

            System.out.println("Deleting records");
            tries = 0;
            do {
                stmt.executeUpdate("delete from public.FldTest2");
                Thread.sleep(100);
                ResultSet rs = stmt.executeQuery("select count(*) from public.FldTest2");
                rs.next();
                recCnt = rs.getInt(1);
                System.out.printf("DELETE left entries in the cache: %d\n", recCnt);
                tries++;
            }
            while (recCnt > 0 && tries < 10);
        }

        System.out.println(">>>>>>>>>>>\n\n\n\n\n");
    }
}
