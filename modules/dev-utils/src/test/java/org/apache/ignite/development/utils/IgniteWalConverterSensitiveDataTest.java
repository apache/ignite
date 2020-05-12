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

package org.apache.ignite.development.utils;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.MetastoreDataRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.CacheObjectImpl;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static java.lang.String.valueOf;
import static java.lang.System.setOut;
import static java.lang.System.setProperty;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_TO_STRING_INCLUDE_SENSITIVE;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.development.utils.IgniteWalConverter.PRINT_RECORDS;
import static org.apache.ignite.development.utils.IgniteWalConverter.SENSITIVE_DATA;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.GridTestUtils.assertNotContains;
import static org.apache.ignite.testframework.wal.record.RecordUtils.isIncludeIntoLog;

/**
 * Class for testing sensitive data when reading {@link WALRecord} using
 * {@link IgniteWalConverter}.
 */
public class IgniteWalConverterSensitiveDataTest extends GridCommonAbstractTest {
    /** Sensitive data prefix. */
    private static final String SENSITIVE_DATA_VALUE_PREFIX = "must_hide_it_";

    /** Path to directory where WAL is stored. */
    private static String walDirPath;

    /** Page size. */
    private static int pageSize;

    /** System out. */
    private static PrintStream sysOut;

    /** Sensitive data values. */
    private static List<String> sensitiveValues = new ArrayList<>();

    /**
     * Test out - can be injected via {@link #injectTestSystemOut()} instead
     * of System.out and analyzed in test.
     */
    private static ByteArrayOutputStream testOut;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        sysOut = System.out;
        testOut = new ByteArrayOutputStream(16 * 1024);

        int nodeId = 0;

        IgniteEx crd = startGrid(nodeId);
        crd.cluster().active(true);

        try (Transaction tx = crd.transactions().txStart()) {
            IgniteCache<Object, Object> cache = crd.cache(DEFAULT_CACHE_NAME);

            sensitiveValues.add(SENSITIVE_DATA_VALUE_PREFIX + 0);
            sensitiveValues.add(SENSITIVE_DATA_VALUE_PREFIX + 1);
            sensitiveValues.add(SENSITIVE_DATA_VALUE_PREFIX + 2);

            String val0 = sensitiveValues.get(0);
            String val1 = sensitiveValues.get(1);
            String val2 = sensitiveValues.get(2);

            cache.put(val0, val0);
            cache.withKeepBinary().put(val1, val1);
            cache.put(val2, new Person(1, val2));

            tx.commit();
        }

        GridKernalContext kernalCtx = crd.context();
        IgniteWriteAheadLogManager wal = kernalCtx.cache().context().wal();

        for (WALRecord walRecord : withSensitiveData()) {
            if (isIncludeIntoLog(walRecord))
                wal.log(walRecord);
        }

        sensitiveValues.add(SENSITIVE_DATA_VALUE_PREFIX);

        wal.flush(null, true);

        IgniteConfiguration cfg = crd.configuration();

        String wd = cfg.getWorkDirectory();
        String wp = cfg.getDataStorageConfiguration().getWalPath();
        String fn = kernalCtx.pdsFolderResolver().resolveFolders().folderName();

        walDirPath = wd + File.separator + wp + File.separator + fn;
        pageSize = cfg.getDataStorageConfiguration().getPageSize();

        stopGrid(nodeId);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        clearGridToStringClassCache();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        log.info("Test output for " + currentTestMethod());
        log.info("----------------------------------------");

        setOut(sysOut);

        log.info(testOut.toString());
        resetTestOut();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(
                new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                    .setAtomicityMode(TRANSACTIONAL)
                    .setQueryEntities(asList(personQueryEntity()))
            )
            .setDataStorageConfiguration(
                new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration().setPersistenceEnabled(true)
                )
            );
    }

    /**
     * Test checks that by default {@link WALRecord} will not be output without
     * system option {@link IgniteWalConverter#PRINT_RECORDS}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNotPrintRecordsByDefault() throws Exception {
        exeWithCheck(false, false, identity());
    }

    /**
     * Test checks that by default sensitive data is displayed.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = PRINT_RECORDS, value = "true")
    public void testShowSensitiveDataByDefault() throws Exception {
        exeWithCheck(true, true, identity());
    }

    /**
     * Test checks that sensitive data is displayed.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = PRINT_RECORDS, value = "true")
    @WithSystemProperty(key = SENSITIVE_DATA, value = "SHOW")
    public void testShowSensitiveData() throws Exception {
        exeWithCheck(true, true, identity());

        setProperty(SENSITIVE_DATA, currentTestMethod().getName());
        resetTestOut();

        exeWithCheck(true, true, identity());
    }

    /**
     * Test verifies that sensitive data will be hidden.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = PRINT_RECORDS, value = "true")
    @WithSystemProperty(key = SENSITIVE_DATA, value = "HIDE")
    @WithSystemProperty(key = IGNITE_TO_STRING_INCLUDE_SENSITIVE, value = "true")
    public void testHideSensitiveData() throws Exception {
        exeWithCheck(false, false, identity());
    }

    /**
     * Test verifies that sensitive data should be replaced with hash.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = PRINT_RECORDS, value = "true")
    @WithSystemProperty(key = SENSITIVE_DATA, value = "HASH")
    public void testHashSensitiveData() throws Exception {
        exeWithCheck(true, false, s -> valueOf(s.hashCode()));
    }

    /**
     * Test verifies that sensitive data should be replaced with MD5 hash.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = PRINT_RECORDS, value = "true")
    @WithSystemProperty(key = SENSITIVE_DATA, value = "MD5")
    public void testMd5HashSensitiveData() throws Exception {
        exeWithCheck(true, false, ProcessSensitiveDataUtils::md5);
    }

    /**
     * Executing {@link IgniteWalConverter} with checking the content of its output.
     *
     * @param containsData Contains or not elements {@link #sensitiveValues} in utility output.
     * @param containsPrefix Contains or not {@link #SENSITIVE_DATA_VALUE_PREFIX} in utility output.
     * @param converter Converting elements {@link #sensitiveValues} for checking in utility output.
     * @throws Exception If failed.
     */
    private void exeWithCheck(
        boolean containsData,
        boolean containsPrefix,
        Function<String, String> converter
    ) throws Exception {
        requireNonNull(converter);

        injectTestSystemOut();

        IgniteWalConverter.main(new String[] {valueOf(pageSize), walDirPath});

        String testOutStr = testOut.toString();

        if (containsPrefix)
            assertContains(log, testOutStr, SENSITIVE_DATA_VALUE_PREFIX);
        else
            assertNotContains(log, testOutStr, SENSITIVE_DATA_VALUE_PREFIX);

        for (String sensitiveDataValue : sensitiveValues) {
            if (containsData)
                assertContains(log, testOutStr, converter.apply(sensitiveDataValue));
            else
                assertNotContains(log, testOutStr, converter.apply(sensitiveDataValue));
        }
    }

    /**
     * Inject {@link #testOut} to System.out for analyze in test.
     */
    private void injectTestSystemOut() {
        setOut(new PrintStream(testOut));
    }

    /**
     * Reset {@link #testOut}.
     */
    private void resetTestOut() {
        testOut.reset();
    }

    /**
     * Creating {@link WALRecord} instances with sensitive data.
     *
     * @return {@link WALRecord} instances with sensitive data.
     */
    private Collection<WALRecord> withSensitiveData() {
        List<WALRecord> walRecords = new ArrayList<>();

        int cacheId = CU.cacheId(DEFAULT_CACHE_NAME);

        DataEntry dataEntry = new DataEntry(
            cacheId,
            new KeyCacheObjectImpl(SENSITIVE_DATA_VALUE_PREFIX, null, 0),
            new CacheObjectImpl(SENSITIVE_DATA_VALUE_PREFIX, null),
            GridCacheOperation.CREATE,
            new GridCacheVersion(),
            new GridCacheVersion(),
            0,
            0,
            0
        );

        byte[] sensitiveDataBytes = SENSITIVE_DATA_VALUE_PREFIX.getBytes(StandardCharsets.UTF_8);

        walRecords.add(new DataRecord(dataEntry));
        walRecords.add(new MetastoreDataRecord(SENSITIVE_DATA_VALUE_PREFIX, sensitiveDataBytes));

        return walRecords;
    }

    /**
     * Create {@link QueryEntity} for {@link Person}.
     *
     * @return QueryEntity for {@link Person}.
     */
    private QueryEntity personQueryEntity() {
        String orgIdField = "orgId";
        String nameField = "name";

        return new QueryEntity()
            .setKeyType(String.class.getName())
            .setValueType(Person.class.getName())
            .addQueryField(orgIdField, Integer.class.getName(), null)
            .addQueryField(nameField, String.class.getName(), null)
            .setIndexes(asList(new QueryIndex(nameField), new QueryIndex(orgIdField)));
    }

    /**
     * Simple class Person for tests.
     */
    private static class Person implements Serializable {
        /** Id organization. */
        int orgId;

        /** Name organization. */
        String name;

        /**
         * Constructor.
         *
         * @param orgId Organization id.
         * @param name Organization name.
         */
        Person(int orgId, String name) {
            this.orgId = orgId;
            this.name = name;
        }
    }
}
