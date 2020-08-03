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

package org.apache.ignite.internal.processors.cache.persistence.metastorage;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Single place to add for basic MetaStorage tests.
 */
public class IgniteMetaStorageBasicTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        DataStorageConfiguration storageCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setMaxSize(100 * 1024 * 1024)
                    .setPersistenceEnabled(true)
            )
            .setWalMode(WALMode.LOG_ONLY);

        cfg.setDataStorageConfiguration(storageCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        super.beforeTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     *
     */
    @Test
    public void testMetaStorageMassivePutFixed() throws Exception {
        IgniteEx ig = startGrid(0);

        ig.cluster().active(true);

        IgniteCacheDatabaseSharedManager db = ig.context().cache().context().database();

        MetaStorage metaStorage = db.metaStorage();

        assertNotNull(metaStorage);

        Random rnd = new Random();

        db.checkpointReadLock();

        int size;
        try {
            for (int i = 0; i < 10_000; i++) {
                size = rnd.nextBoolean() ? 3500 : 2 * 3500;
                String key = "TEST_KEY_" + (i % 1000);

                byte[] arr = new byte[size];
                rnd.nextBytes(arr);

                metaStorage.remove(key);

                metaStorage.writeRaw(key, arr/*b.toString().getBytes()*/);
            }
        }
        finally {
            db.checkpointReadUnlock();
        }
    }

    /**
     *
     */
    @Test
    public void testMetaStorageMassivePutRandom() throws Exception {
        IgniteEx ig = startGrid(0);

        ig.cluster().active(true);

        IgniteCacheDatabaseSharedManager db = ig.context().cache().context().database();

        MetaStorage metaStorage = db.metaStorage();

        assertNotNull(metaStorage);

        Random rnd = new Random();

        db.checkpointReadLock();

        int size;
        try {
            for (int i = 0; i < 50_000; i++) {
                size = 100 + rnd.nextInt(9000);

                String key = "TEST_KEY_" + (i % 2_000);

                byte[] arr = new byte[size];
                rnd.nextBytes(arr);

                metaStorage.remove(key);

                metaStorage.writeRaw(key, arr);
            }
        }
        finally {
            db.checkpointReadUnlock();
        }

        stopGrid();
    }

    /**
     * @param metaStorage Meta storage.
     * @param size Size.
     */
    private Map<String, byte[]> putDataToMetaStorage(MetaStorage metaStorage, int size, int from) throws IgniteCheckedException {
        Map<String, byte[]> res = new HashMap<>();

        for (Iterator<IgniteBiTuple<String, byte[]>> it = generateTestData(size, from).iterator(); it.hasNext(); ) {
            IgniteBiTuple<String, byte[]> d = it.next();

            metaStorage.write(d.getKey(), d.getValue());

            res.put(d.getKey(), d.getValue());
        }

        return res;
    }

    /**
     * Testing data migration between metastorage partitions (delete partition case)
     */
    @Test
    public void testDeletePartitionFromMetaStorageMigration() throws Exception {
        final Map<String, byte[]> testData = new HashMap<>();

        MetaStorage.PRESERVE_LEGACY_METASTORAGE_PARTITION_ID = true;

        try {
            IgniteEx ig = startGrid(0);

            ig.cluster().active(true);

            IgniteCacheDatabaseSharedManager db = ig.context().cache().context().database();

            MetaStorage metaStorage = db.metaStorage();

            assertNotNull(metaStorage);

            db.checkpointReadLock();

            try {
                testData.putAll(putDataToMetaStorage(metaStorage, 1_000, 0));
            }
            finally {
                db.checkpointReadUnlock();
            }

            db.waitForCheckpoint("Test");

            enableCheckpoints(ig, false);

            db.checkpointReadLock();

            try {
                testData.putAll(putDataToMetaStorage(metaStorage, 1_000, 1_000));
            }
            finally {
                db.checkpointReadUnlock();
            }

            stopGrid(0);

            MetaStorage.PRESERVE_LEGACY_METASTORAGE_PARTITION_ID = false;

            IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(0));

            cfg.getDataStorageConfiguration().setCheckpointFrequency(3600 * 1000L);

            ig = (IgniteEx)startGrid(getTestIgniteInstanceName(0), optimize(cfg), null);

            ig.cluster().active(true);

            db = ig.context().cache().context().database();

            metaStorage = db.metaStorage();

            assertNotNull(metaStorage);

            db.checkpointReadLock();

            try {
                testData.putAll(putDataToMetaStorage(metaStorage, 1_000, 2_000));
            }
            finally {
                db.checkpointReadUnlock();
            }

            db.waitForCheckpoint("Test");

            stopGrid(0);

            ig = startGrid(0);

            ig.cluster().active(true);

            db = ig.context().cache().context().database();

            metaStorage = db.metaStorage();

            assertNotNull(metaStorage);

            db.checkpointReadLock();
            try {
                Collection<IgniteBiTuple<String, byte[]>> read = readTestData(testData, metaStorage);

                int cnt = 0;
                for (IgniteBiTuple<String, byte[]> r : read) {
                    byte[] test = testData.get(r.get1());

                    Assert.assertArrayEquals(r.get2(), test);

                    cnt++;
                }

                assertEquals(cnt, testData.size());
            }
            finally {
                db.checkpointReadUnlock();
            }
        }
        finally {
            MetaStorage.PRESERVE_LEGACY_METASTORAGE_PARTITION_ID = false;
        }

    }

    /**
     * Testing data migration between metastorage partitions
     */
    @Test
    public void testMetaStorageMigration() throws Exception {
        final Map<String, byte[]> testData = new HashMap<>(5_000);

        generateTestData(5_000, -1).forEach(t -> testData.put(t.get1(), t.get2()));

        MetaStorage.PRESERVE_LEGACY_METASTORAGE_PARTITION_ID = true;

        try {
            IgniteEx ig = startGrid(0);

            ig.cluster().active(true);

            IgniteCacheDatabaseSharedManager db = ig.context().cache().context().database();

            MetaStorage metaStorage = db.metaStorage();

            assertNotNull(metaStorage);

            db.checkpointReadLock();

            try {
                for (Map.Entry<String, byte[]> v : testData.entrySet())
                    metaStorage.write(v.getKey(), v.getValue());
            }
            finally {
                db.checkpointReadUnlock();
            }

            stopGrid(0);

            MetaStorage.PRESERVE_LEGACY_METASTORAGE_PARTITION_ID = false;

            ig = startGrid(0);

            ig.cluster().active(true);

            db = ig.context().cache().context().database();

            metaStorage = db.metaStorage();

            assertNotNull(metaStorage);

            db.checkpointReadLock();

            try {
                Collection<IgniteBiTuple<String, byte[]>> read = readTestData(testData, metaStorage);

                int cnt = 0;
                for (IgniteBiTuple<String, byte[]> r : read) {
                    byte[] test = testData.get(r.get1());

                    Assert.assertArrayEquals(r.get2(), test);

                    cnt++;
                }

                assertEquals(cnt, testData.size());
            }
            finally {
                db.checkpointReadUnlock();
            }
        }
        finally {
            MetaStorage.PRESERVE_LEGACY_METASTORAGE_PARTITION_ID = false;
        }
    }

    /**
     * Testing temporary storage
     */
    @Test
    public void testMetaStoreMigrationTmpStorage() throws Exception {
        List<IgniteBiTuple<String, byte[]>> data = generateTestData(2_000, -1).collect(Collectors.toList());

        // memory
        try (MetaStorage.TmpStorage tmpStorage = new MetaStorage.TmpStorage(4 * 1024 * 1024, log)) {
            for (IgniteBiTuple<String, byte[]> item : data)
                tmpStorage.add(item.get1(), item.get2());

            compare(tmpStorage.stream().iterator(), data.iterator());
        }

        // file
        try (MetaStorage.TmpStorage tmpStorage = new MetaStorage.TmpStorage(4 * 1024, log)) {
            for (IgniteBiTuple<String, byte[]> item : data)
                tmpStorage.add(item.get1(), item.get2());

            compare(tmpStorage.stream().iterator(), data.iterator());
        }
    }

    /**
     * Test data generation
     */
    private static Stream<IgniteBiTuple<String, byte[]>> generateTestData(int size, int fromKey) {
        final AtomicInteger idx = new AtomicInteger(fromKey);
        final Random rnd = new Random();

        return Stream.generate(() -> {
            byte[] val = new byte[1024];

            rnd.nextBytes(val);

            return new IgniteBiTuple<>("KEY_" + (fromKey < 0 ? rnd.nextInt() : idx.getAndIncrement()), val);
        }).limit(size);
    }

    /**
     * Compare two iterator
     *
     * @param it It.
     * @param it1 It 1.
     */
    private static void compare(Iterator<IgniteBiTuple<String, byte[]>> it, Iterator<IgniteBiTuple<String, byte[]>> it1) {
        while (true) {
            Assert.assertEquals(it.hasNext(), it1.hasNext());

            if (!it.hasNext())
                break;

            IgniteBiTuple<String, byte[]> i = it.next();
            IgniteBiTuple<String, byte[]> i1 = it1.next();

            Assert.assertEquals(i.get1(), i.get1());

            Assert.assertArrayEquals(i.get2(), i1.get2());
        }
    }

    /**
     * Verifies that MetaStorage after massive amounts of keys stored and updated keys restores its state successfully
     * after restart.
     *
     * See <a hfer="https://issues.apache.org/jira/browse/IGNITE-7964" target="_top">IGNITE-7964</a> for more context
     * about this test.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMetaStorageMassivePutUpdateRestart() throws Exception {
        IgniteEx ig = startGrid(0);

        ig.cluster().active(true);

        final byte KEYS_CNT = 100;
        final String KEY_PREFIX = "test.key.";
        final String NEW_VAL_PREFIX = "new.val.";
        final String UPDATED_VAL_PREFIX = "updated.val.";

        loadKeys(ig, KEYS_CNT, KEY_PREFIX, NEW_VAL_PREFIX, UPDATED_VAL_PREFIX);

        stopGrid(0);

        ig = startGrid(0);

        ig.cluster().active(true);

        verifyKeys(ig, KEYS_CNT, KEY_PREFIX, UPDATED_VAL_PREFIX);
    }

    /**
     * @throws Exception If fails.
     */
    @Test
    public void testRecoveryOfMetastorageWhenNodeNotInBaseline() throws Exception {
        IgniteEx ig0 = startGrid(0);

        ig0.cluster().active(true);

        final byte KEYS_CNT = 100;
        final String KEY_PREFIX = "test.key.";
        final String NEW_VAL_PREFIX = "new.val.";
        final String UPDATED_VAL_PREFIX = "updated.val.";

        startGrid(1);

        // Disable checkpoints in order to check whether recovery works.
        forceCheckpoint(grid(1));
        enableCheckpoints(grid(1), false);

        loadKeys(grid(1), KEYS_CNT, KEY_PREFIX, NEW_VAL_PREFIX, UPDATED_VAL_PREFIX);

        stopGrid(1, true);

        startGrid(1);

        verifyKeys(grid(1), KEYS_CNT, KEY_PREFIX, UPDATED_VAL_PREFIX);
    }

    /**
     * @throws Exception If fails.
     */
    @Test
    public void testReadOnlyIterationOrder() throws Exception {
        IgniteEx ignite = startGrid(0);

        ignite.cluster().active(true);

        MetaStorage storage = ignite.context().cache().context().database().metaStorage();

        ignite.context().cache().context().database().checkpointReadLock();

        try {
            storage.write("a", 0);

            storage.write("z", 0);

            storage.write("pref-1", 1);

            storage.write("pref-3", 3);

            storage.write("pref-5", 5);

            storage.write("pref-7", 7);

            GridTestUtils.setFieldValue(storage, "readOnly", true);

            storage.applyUpdate("pref-0", JdkMarshaller.DEFAULT.marshal(0));

            storage.applyUpdate("pref-1", JdkMarshaller.DEFAULT.marshal(10));

            storage.applyUpdate("pref-4", JdkMarshaller.DEFAULT.marshal(4));

            storage.applyUpdate("pref-5", null);

            storage.applyUpdate("pref-8", JdkMarshaller.DEFAULT.marshal(8));

            List<String> keys = new ArrayList<>();

            List<Integer> values = new ArrayList<>();

            storage.iterate("pref", (key, val) -> {
                keys.add(key);

                values.add((Integer)val);
            }, true);

            assertEqualsCollections(
                Arrays.asList("pref-0", "pref-1", "pref-3", "pref-4", "pref-7", "pref-8"),
                keys
            );

            assertEqualsCollections(
                Arrays.asList(0, 10, 3, 4, 7, 8),
                values
            );
        }
        finally {
            ignite.context().cache().context().database().checkpointReadUnlock();
        }
    }

    /** */
    private void loadKeys(IgniteEx ig,
        byte keysCnt,
        String keyPrefix,
        String newValPrefix,
        String updatedValPrefix
    ) throws IgniteCheckedException {
        IgniteCacheDatabaseSharedManager db = ig.context().cache().context().database();

        MetaStorage metaStorage = db.metaStorage();

        db.checkpointReadLock();
        try {
            for (byte i = 0; i < keysCnt; i++)
                metaStorage.write(keyPrefix + i, newValPrefix + i);

            for (byte i = 0; i < keysCnt; i++)
                metaStorage.write(keyPrefix + i, updatedValPrefix + i);
        }
        finally {
            db.checkpointReadUnlock();
        }
    }

    /** */
    private void verifyKeys(IgniteEx ig,
        byte keysCnt,
        String keyPrefix,
        String valPrefix
    ) throws IgniteCheckedException {
        MetaStorage metaStorage = ig.context().cache().context().database().metaStorage();

        for (byte i = 0; i < keysCnt; i++) {
            Serializable val = metaStorage.read(keyPrefix + i);

            Assert.assertEquals(valPrefix + i, val);
        }
    }

    /** */
    private Collection<IgniteBiTuple<String, byte[]>> readTestData(
        Map<String, byte[]> testData,
        MetaStorage metaStorage
    ) throws IgniteCheckedException {
        Collection<IgniteBiTuple<String, byte[]>> read = new ArrayList<>();

        metaStorage.iterate("", (key, val) -> {
            if (testData.containsKey(key))
                read.add(new IgniteBiTuple<String, byte[]>(key, ((byte[])val)));
        }, true);

        return read;
    }
}
