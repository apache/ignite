/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.jdbc.thin;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Random;
import java.util.UUID;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.sqltests.SqlDataTypesCoverageTests;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Data types coverage for basic jdbc-put-cache-retrieve operations.
 */
public class JdbcThinJdbcToCacheDataTypesCoverageTest extends SqlDataTypesCoverageTests {
    /** Signals that tests should start in affinity awareness mode. */
    public static boolean affinityAwareness;

    /** URL. */
    private String url = affinityAwareness ?
        "jdbc:ignite:thin://127.0.0.1:10800..10802?affinityAwareness=true" :
        "jdbc:ignite:thin://127.0.0.1?affinityAwareness=false";

    /** Connection. */
    private Connection conn;

    /** Statement. */
    private Statement stmt;

    /** @inheritDoc */
    @SuppressWarnings("RedundantMethodOverride")
    @Before
    @Override
    public void init() throws Exception {
        super.init();

        conn = DriverManager.getConnection(url);

        stmt = conn.createStatement();

        assert stmt != null;
        assert !stmt.isClosed();
    }

    /**
     * Cleanup.
     *
     * @throws Exception If Failed.
     */
    @After
    public void tearDown() throws Exception {
        if (stmt != null && !stmt.isClosed()) {
            stmt.close();

            assert stmt.isClosed();
        }

        if (conn != null && !conn.isClosed()) {
            conn.close();

            assert conn.isClosed();
        }
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testTimeDataType() throws Exception {
        super.testTimeDataType();
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testSmallIntDataType() throws Exception {
        super.testSmallIntDataType();
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testBinaryDataType() throws Exception {
        super.testBinaryDataType();
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testGeometryDataType() throws Exception {
        super.testGeometryDataType();
    }

    /** {@inheritDoc} */
    @Ignore("https://ggsystems.atlassian.net/browse/GG-23665")
    @Test
    @Override public void testDateDataType() throws Exception {
        super.testDateDataType();
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testTinyIntDataType() throws Exception {
        super.testTinyIntDataType();
    }

    /**
     * Create cache/table based on test-parameters-dependent template.
     * Verify that cache.get returns same data that were inserted to cache via jdbc thin insert.
     *
     * @param dataType Sql data type to check.
     * @param valsToCheck Array of values to check.
     * @throws Exception If Failed.
     */
    @SuppressWarnings("unchecked")
    @Override protected void checkBasicSqlOperations(SqlDataType dataType, Object... valsToCheck) throws Exception {
        assert valsToCheck.length > 0;

        IgniteEx ignite =
            (cacheMode == CacheMode.LOCAL || writeSyncMode == CacheWriteSynchronizationMode.PRIMARY_SYNC) ?
                grid(0) :
                grid(new Random().nextInt(NODES_CNT));

        String uuidPostfix = UUID.randomUUID().toString().replaceAll("-", "_");

        final String tblName = "table" + uuidPostfix;

        final String templateName = "template" + uuidPostfix;

        final String cacheName = "cache" + uuidPostfix;

        final String idxName = "idx" + uuidPostfix;

        CacheConfiguration cfg = new CacheConfiguration<>(templateName)
            .setAtomicityMode(atomicityMode)
            .setCacheMode(cacheMode)
            .setExpiryPolicyFactory(ttlFactory)
            .setBackups(backups)
            .setEvictionPolicyFactory(evictionFactory)
            .setOnheapCacheEnabled(evictionFactory != null || onheapCacheEnabled)
            .setWriteSynchronizationMode(writeSyncMode)
            .setAffinity(new RendezvousAffinityFunction(false, PARTITIONS_CNT));

        ignite.addCacheConfiguration(cfg);

        stmt.execute("CREATE TABLE " + tblName +
            "(id " + dataType + " PRIMARY KEY," +
            " val " + dataType + ")" +
            " WITH " + "\"template=" + templateName + ",cache_name=" + cacheName + ",wrap_value=false\"");

        if (cacheMode != CacheMode.LOCAL)
            stmt.execute("CREATE INDEX " + idxName + " ON " + tblName + "(id, val)");

        for (Object valToCheck : valsToCheck) {
            Object sqlStrVal = valToCheck instanceof SqlStrConvertedValHolder ?
                ((SqlStrConvertedValHolder)valToCheck).sqlStrVal() :
                valToCheck;

            Object orignialVal = valToCheck instanceof SqlStrConvertedValHolder ?
                ((SqlStrConvertedValHolder)valToCheck).originalVal() :
                valToCheck;

            // INSERT.
            stmt.execute("INSERT INTO " + tblName + "(id, val)  VALUES (" + sqlStrVal + ", " + sqlStrVal + ");");

            if (writeSyncMode == CacheWriteSynchronizationMode.FULL_ASYNC &&
                !waitForCondition(() -> ignite.cache(cacheName).get(orignialVal) != null,
                    TIMEOUT_FOR_KEY_RETRIEVAL_IN_FULL_ASYNC_MODE))
                fail("Unable to retrieve data via cache get.");

            // Check with cache get.
            Object gotVal = ignite.cache(cacheName).get(orignialVal);

            assertNotNull(gotVal);

            assertEquals("Unexpected data type found.", orignialVal.getClass(), gotVal.getClass());

            if (orignialVal instanceof byte[])
                assertTrue("Unexpected value found.", Arrays.equals((byte[])orignialVal, (byte[])gotVal));
            else
                assertEquals("Unexpected value found.", orignialVal, gotVal);

            // cleanup
            ignite.cache(cacheName).remove(orignialVal);

            if (writeSyncMode == CacheWriteSynchronizationMode.FULL_ASYNC &&
                !waitForCondition(() -> ignite.cache(cacheName).get(orignialVal) == null,
                    TIMEOUT_FOR_KEY_RETRIEVAL_IN_FULL_ASYNC_MODE))
                fail("Deleted data are still retrievable via cache.get().");

            assertNull(ignite.cache(cacheName).get(orignialVal));
        }
    }
}
