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
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_SQL_PARSER_DISABLE_H2_FALLBACK;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * COPY statement tests.
 */
public class JdbcThinBulkLoadPerfTest extends JdbcThinAbstractDmlStatementSelfTest {
    /** A database to test. */
    private enum DB {
        /** Apache Ignite. */
        IGNITE,

        /** PostgresQL. */
        POSTGRESQL,

        /** MySQL. */
        MYSQL
    }

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
    protected boolean nearCache() { return false; }

    /**
     * Returns cache atomicity mode we are testing.
     *
     * @return The cache atomicity mode we are testing.
     */
    protected CacheAtomicityMode atomicityMode() { return CacheAtomicityMode.ATOMIC; }

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

    /**
     * Generates a simple CSV file with required number of records, fields, and number of characters in a field.
     * <p>
     * Default separators and simple contents are used here.
     *
     * @param fileName The file name to write CSV file to.
     * @param lineNum The number of lines (records).
     * @param fldNum The number of fields per line.
     * @param fldSize The number of characters in the field.
     * @throws FileNotFoundException If the file cannot be written to.
     */
    private static void generateCsv(String fileName, int lineNum, int fldNum, int fldSize) throws FileNotFoundException {
        try (PrintWriter pw = new PrintWriter(new FileOutputStream(fileName))) {
            StringBuilder sb = new StringBuilder(fldNum * (fldSize + 3));

            String lineNumFmt = "%" + String.format("%d", lineNum).length() + "s";

            String fldFmt = "%0" + fldSize + "d";

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

    /**
     * Tests Ignite COPY command performance on 1M records file with 10 fields 10 characters each.
     *
     * @throws Exception If failed.
     */
    public void testIgnitePerf_1M_10_10_10() throws Exception {
        checkPerf(DB.IGNITE, 1_000_000, 10, 10, 10);
    }

    /**
     * Tests performance of COPY command by importing a CSV file.
     *
     * @param db The database type to check.
     * @param lineCnt Record (line) count in CSV file.
     * @param fileFldCnt Number of fields in CSV file.
     * @param tblFldCnt Number of fields in the database.
     * @param fldSize Field size in CSV file in characters.
     * @throws Exception If failed.
     */
    public void checkPerf(DB db, int lineCnt, int tblFldCnt, int fileFldCnt, int fldSize) throws Exception {
        String tableName;

        switch (db) {
            case IGNITE:
                tableName = "public.FldTest";

                break;

            case POSTGRESQL:
                conn = DriverManager.getConnection("jdbc:postgresql://localhost/postgres",
                    "postgres", "admin");

                stmt = conn.createStatement();

                tableName = "public.FldTest";

                break;

            case MYSQL:
                Class.forName("com.mysql.cj.jdbc.Driver");

                conn = DriverManager.getConnection("jdbc:mysql://localhost/mysql?serverTimezone=UTC",
                    "mysql", "admin");

                stmt = conn.createStatement();

                tableName = "FldTest";

                break;

            default:
                throw new IllegalArgumentException();
        }

        String workDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), "work", false).getAbsolutePath();
        String fileName = workDir + "/testfile_" + lineCnt + "_" + fileFldCnt + "_" + fldSize + ".csv";

        if (!Files.exists(Paths.get(fileName))) {
            System.out.println("Generating test file");

            generateCsv(fileName, lineCnt, fileFldCnt, fldSize);
        }

        StringBuilder fieldsCreate = new StringBuilder("f000000 VARCHAR(" + fldSize + ") PRIMARY KEY");

        for (int i = 1; i < tblFldCnt; ++i)
            fieldsCreate.append(String.format(",f%06d VARCHAR(%d)", i, fldSize));

        StringBuilder fieldsInsert = new StringBuilder("f000000");

        for (int i = 1; i < tblFldCnt; ++i)
            fieldsInsert.append(String.format(",f%06d", i));

        String sql;

        switch (db) {
            case IGNITE:
                sql = "copy from \"" + fileName + "\" into " + tableName + " " + "(" + fieldsInsert.toString() +
                    ") format csv";
                break;

            case POSTGRESQL:
                sql = "copy " + tableName + " " + "(" + fieldsInsert.toString() + ") from '" + fileName +
                    "' with (format csv)";
                break;

            case MYSQL:
                sql = "load data local infile '" + fileName + "' into table " + tableName;
                break;

            default:
                throw new IllegalArgumentException();
        }

        System.out.println("Creating table");

        stmt.executeUpdate("drop table if exists " + tableName);

        for (int i = 0; i < 10; ++i) {
            stmt.executeUpdate("create table " + tableName + " (" + fieldsCreate.toString() + ")");

            System.out.println("Running COPY");

            long startNs = System.nanoTime();

            long updateRecCnt = stmt.executeUpdate(sql);

            long stopNs = System.nanoTime();

            System.out.println("COPY done. Update count: " + updateRecCnt + ". Checking records count with SELECT");

            ResultSet rs = stmt.executeQuery("select count(*) from " + tableName);

            rs.next();
            long selectRecCnt = rs.getInt(1);

            System.out.printf("Select reported lines: %d\n", selectRecCnt);

            double elapsedSec = 1e-9d * (stopNs - startNs);

            System.out.print("\n\n\n>>>>>>>>>>>\n" +
                "    Elapsed  : " + String.format("%12.9f sec", elapsedSec) + "\n" +
                "    Lines    : " + String.format("%12d", lineCnt) +
                    " => " + updateRecCnt + "(copy) => " + selectRecCnt + " (select)\n" +
                "    Lines/sec: " + String.format("%12.0f", (lineCnt / elapsedSec)) + "\n" +
                "    Sec/line : " + String.format("%12.3f μsec", 1e6d * (elapsedSec / lineCnt)) + "\n" +
                "\n\n\n");

            System.out.println("Dropping table");

            stmt.executeUpdate("drop table " + tableName);
        }
    }
}
