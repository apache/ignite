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

package org.apache.ignite.internal.processors.database;

import java.io.File;
import java.io.IOException;
import java.nio.file.OpenOption;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ModifiedExpiryPolicy;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.tree.CacheDataTree;
import org.apache.ignite.internal.processors.cache.tree.SearchRow;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_DATA_REG_DEFAULT_NAME;
import static org.apache.ignite.internal.processors.cache.persistence.DataRegionMetricsImpl.DATAREGION_METRICS_PREFIX;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;

/**
 * Tests for multi-page in-place update.
 */
public class MultiPageInPlaceUpdateTest extends GridCommonAbstractTest {
    /** */
    private boolean pds;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setFileIOFactory(new FailingFileIOFactory())
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(pds)
                .setMetricsEnabled(true)
            )
        );

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** */
    @Test
    public void testInPlaceUpdateInMemory() throws Exception {
        checkInPlaceUpdate(false);
    }

    /** */
    @Test
    public void testInPlaceUpdatePersistence() throws Exception {
        checkInPlaceUpdate(true);
    }

    /** */
    private void checkInPlaceUpdate(boolean pds) throws Exception {
        this.pds = pds;

        int entrySize = 100 * 1024;

        IgniteEx ignite = startGrid(0);

        if (pds)
            ignite.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Integer, byte[]> cache = ignite.getOrCreateCache(DEFAULT_CACHE_NAME);

        byte[] payload = new byte[entrySize];

        ThreadLocalRandom.current().nextBytes(payload);

        int key = 0;

        cache.put(key, payload);

        long link = link(ignite, key);

        for (int i = 0; i < 100; i++) {
            payload[ThreadLocalRandom.current().nextInt(entrySize)] = (byte)i;

            cache.put(key, payload);

            assertEqualsArraysAware(payload, cache.get(0));

            assertEquals(link, link(ignite, key));
        }

        // Size changed, can't do in-place update.
        cache.put(key, new byte[payload.length + 1]);

        assertNotSame(link, link(ignite, key));
    }

    /** */
    @Test
    public void testUpdateDifferentSizesInMemory() throws Exception {
        checkUpdateDifferentSizes(false);
    }

    /** */
    @Test
    public void testUpdateDifferentSizesPersistence() throws Exception {
        checkUpdateDifferentSizes(true);
    }

    /** */
    public void checkUpdateDifferentSizes(boolean pds) throws Exception {
        this.pds = pds;

        IgniteEx ignite = startGrid(0);

        if (pds)
            ignite.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Integer, byte[]> cache = ignite.getOrCreateCache(DEFAULT_CACHE_NAME);

        int pageSize = ignite.context().cache().context().database().pageSize();

        for (int i = 1000; i < pageSize; i++)
            checkLinkChange(ignite, cache, i, true, false);
    }


    /** */
    @Test
    public void testDirtyPagesCountAfterUpdate() throws Exception {
        pds = true;
        int entrySize = 100 * 1024;
        int entryCnt = 100;

        IgniteEx ignite = startGrid(0);

        ignite.cluster().state(ClusterState.ACTIVE);
        IgniteCache<Integer, byte[]> cache = ignite.getOrCreateCache(DEFAULT_CACHE_NAME);

        LongMetric dirtyPages = ignite.context().metric().registry(metricName(DATAREGION_METRICS_PREFIX,
            DFLT_DATA_REG_DEFAULT_NAME)).findMetric("DirtyPages");

        int pageSize = ignite.context().cache().context().database().pageSize();

        byte[] payload = new byte[entrySize];

        for (int i = 0; i < entryCnt; i++)
            cache.put(i, payload);

        assertTrue(dirtyPages.value() >= entryCnt * entrySize / pageSize);

        forceCheckpoint();

        // Update only first page.
        payload[0] = 1;

        for (int i = 0; i < entryCnt; i++)
            cache.put(i, payload);

        assertTrue(dirtyPages.value() < entryCnt * entrySize / pageSize);

        for (int i = 0; i < entryCnt; i++)
            assertEqualsArraysAware(payload, cache.get(i));

        forceCheckpoint();

        // Update only last page.
        payload[entrySize - 1] = 1;

        for (int i = 0; i < entryCnt; i++)
            cache.put(i, payload);

        assertTrue(dirtyPages.value() < entryCnt * entrySize / pageSize);

        for (int i = 0; i < entryCnt; i++)
            assertEqualsArraysAware(payload, cache.get(i));

        // Update some intermediate page.
        payload[entrySize / 2] = 1;

        for (int i = 0; i < entryCnt; i++)
            cache.put(i, payload);

        assertTrue(dirtyPages.value() < entryCnt * entrySize / pageSize);

        for (int i = 0; i < entryCnt; i++)
            assertEqualsArraysAware(payload, cache.get(i));
    }

    /** */
    @Test
    public void testApplyInPlaceUpdateDeltaRecordsAfterCrash() throws Exception {
        pds = true;
        int entrySize = 100 * 1024;
        int entryCnt = 100;

        IgniteEx ignite = startGrid(0);

        ignite.cluster().state(ClusterState.ACTIVE);
        IgniteCache<Integer, byte[]> cache = ignite.getOrCreateCache(DEFAULT_CACHE_NAME);

        byte[] payload = new byte[entrySize];
        long[] links = new long[entryCnt];

        for (int i = 0; i < entryCnt; i++) {
            cache.put(i, payload);
            links[i] = link(ignite, i);
        }

        forceCheckpoint();

        byte[][] payloads = new byte[entryCnt][entrySize];

        for (int i = 0; i < entryCnt / 2; i++) {
            // First change of the page - produce page snapshot WAL record.
            ThreadLocalRandom.current().nextBytes(payloads[i]);
            cache.put(i, payloads[i]);
            assertEquals(links[i], link(ignite, i));
            // Second change of the page - produce delta WAL record.
            ThreadLocalRandom.current().nextBytes(payloads[i]);
            cache.put(i, payloads[i]);
            assertEquals(links[i], link(ignite, i));
        }

        int pageSize = ignite.context().cache().context().database().pageSize();

        for (int i = entryCnt / 2; i < entryCnt; i++) {
            // Randomly modify pages of entry.
            // - Some pages can produce only page snapshot.
            // - Some pages can produce both page snapshot and delta pages.
            // - Some pages can be untouched.
            for (int j = 0; j < entrySize / pageSize; j++) {
                payloads[i][ThreadLocalRandom.current().nextInt(entrySize)] = (byte)j;
                cache.put(i, payloads[i]);
                assertEquals(links[i], link(ignite, i));
            }
        }

        FailingFileIOFactory failingFactory = (FailingFileIOFactory)ignite.configuration()
            .getDataStorageConfiguration().getFileIOFactory();

        failingFactory.failFlag.set(true);

        try {
            forceCheckpoint();

            fail("Expected failure on checkpoint");
        }
        catch (Exception ignore) {
            // Expected.
        }

        stopGrid(0);
        ignite = startGrid(0);

        cache = ignite.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < entryCnt; i++) {
            assertEqualsArraysAware(payloads[i], cache.get(i));
            assertEquals(links[i], link(ignite, i));
        }
    }

    /** */
    @Test
    public void testLogicalRecoveryInPlaceUpdatedEntriesAfterCrash() throws Exception {
        pds = true;
        int entrySize = 100 * 1024;
        int entryCnt = 100;

        IgniteEx ignite = startGrid(0);

        ignite.cluster().state(ClusterState.ACTIVE);
        IgniteCache<Integer, byte[]> cache = ignite.getOrCreateCache(DEFAULT_CACHE_NAME);

        byte[] payload = new byte[entrySize];
        long[] links = new long[entryCnt];

        for (int i = 0; i < entryCnt; i++) {
            cache.put(i, payload);
            links[i] = link(ignite, i);
        }

        forceCheckpoint();

        byte[][] payloads = new byte[entryCnt][entrySize];
        for (int i = 0; i < entryCnt; i++) {
            ThreadLocalRandom.current().nextBytes(payloads[i]);
            cache.put(i, payloads[i]);
            assertEquals(links[i], link(ignite, i));
        }

        stopGrid(0, true);
        ignite = startGrid(0);

        cache = ignite.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < entryCnt; i++) {
            assertEqualsArraysAware(payloads[i], cache.get(i));
            // Entry has the same payload, but link can be changed, since logical recovery applies records never using
            // in-place update.
            assertNotSame(links[i], link(ignite, i));
        }
    }

    /** */
    @Test
    public void testInPlaceUpdateWithTtl() throws Exception {
        IgniteEx ignite = startGrid(0);

        IgniteCache<Integer, byte[]> cache = ignite.getOrCreateCache(DEFAULT_CACHE_NAME);
        cache = cache.withExpiryPolicy(new CreatedExpiryPolicy(Duration.ONE_DAY));

        // In-place update is enabled when TTL is not changed and entry occupies only one page.
        checkLinkChange(ignite, cache, 100, true, false);

        // In-place update is disabled when TTL is not changed, but entry occupies more than one page.
        checkLinkChange(ignite, cache, ignite.context().cache().context().database().pageSize(), false, false);

        cache = cache.withExpiryPolicy(new ModifiedExpiryPolicy(Duration.ONE_DAY));

        // In-place update is disabled when TTL is changed.
        checkLinkChange(ignite, cache, 100, false, true);
    }

    /** */
    private void checkLinkChange(
        IgniteEx ignite,
        IgniteCache<Integer, byte[]> cache,
        int payloadSize,
        boolean expectInPlaceUpdate,
        boolean ensureTtlChanged
    ) throws IgniteCheckedException {
        int key = 0;

        byte[] payload = new byte[payloadSize];
        ThreadLocalRandom.current().nextBytes(payload);

        cache.put(key, payload);
        long link = link(ignite, key);

        long ts = U.currentTimeMillis();

        if (ensureTtlChanged) {
            while (ts == U.currentTimeMillis())
                doSleep(10);
        }

        ThreadLocalRandom.current().nextBytes(payload);
        cache.put(key, payload);

        assertEquals(expectInPlaceUpdate, link == link(ignite, key));
        assertEqualsArraysAware(payload, cache.get(key));
    }

    /** */
    private long link(IgniteEx ignite, Object key) throws IgniteCheckedException {
        KeyCacheObject keyCacheObj = ignite.cachex(DEFAULT_CACHE_NAME).context().toCacheKeyObject(key);
        SearchRow searchRow = new SearchRow(CU.cacheId(DEFAULT_CACHE_NAME), keyCacheObj);

        CacheDataTree tree = ignite.cachex(DEFAULT_CACHE_NAME).context().topology()
            .localPartition(keyCacheObj.partition()).dataStore().tree();

        assertNotNull(tree);

        CacheDataRow row = tree.findOne(searchRow);
        assertNotNull(row);

        return row.link();
    }

    /** */
    private static final class FailingFileIOFactory implements FileIOFactory {
        /** */
        private final FileIOFactory delegateFactory;

        /** */
        private final AtomicBoolean failFlag = new AtomicBoolean();

        /** */
        FailingFileIOFactory() {
            delegateFactory = new RandomAccessFileIOFactory();
        }

        /** {@inheritDoc} */
        @Override public FileIO create(File file, OpenOption... modes) throws IOException {
            FileIO delegate = delegateFactory.create(file, modes);

            if (failFlag.get() && file.getName().contains("END.bin"))
                throw new IOException("Test exception");

            return delegate;
        }
    }
}
