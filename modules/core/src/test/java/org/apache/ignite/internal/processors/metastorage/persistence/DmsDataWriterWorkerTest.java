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

package org.apache.ignite.internal.processors.metastorage.persistence;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.testframework.junits.logger.GridTestLog4jLogger;
import org.apache.ignite.thread.IgniteThread;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.COMMON_KEY_PREFIX;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.cleanupGuardKey;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.historyItemKey;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.localKey;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.versionKey;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageVersion.INITIAL_VERSION;
import static org.apache.ignite.internal.processors.metastorage.persistence.DmsDataWriterWorker.DUMMY_VALUE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/** */
public class DmsDataWriterWorkerTest {
    /** */
    private static IgniteLogger log = new GridTestLog4jLogger(true).getLogger(DmsDataWriterWorkerTest.class);

    /** */
    private Thread testThread;

    /** */
    private MockDmsLocalMetaStorageLock lock;

    /** */
    private MyReadWriteMetaStorageMock metastorage;

    /** */
    private DmsDataWriterWorker worker;

    /** */
    @Before
    public void before() {
        testThread = Thread.currentThread();

        lock = new MockDmsLocalMetaStorageLock();

        metastorage = new MyReadWriteMetaStorageMock();

        worker = new DmsDataWriterWorker(
            DmsDataWriterWorkerTest.class.getSimpleName(),
            log,
            lock,
            throwable -> {}
        );

        worker.setMetaStorage(metastorage);
    }

    /** */
    @After
    public void after() throws InterruptedException {
        worker.cancel(true);
    }

    /** */
    @Test
    public void testRestoreFromEmptyStorage() throws Exception {
        startWorker();

        stopWorker();

        assertEquals(INITIAL_VERSION, metastorage.read(versionKey()));
    }

    /** */
    @Test
    public void testRestoreAfterFailedCleanup() throws Exception {
        metastorage.write(cleanupGuardKey(), DUMMY_VALUE);

        metastorage.write(COMMON_KEY_PREFIX + "dummy1", "val1");
        metastorage.write(COMMON_KEY_PREFIX + "dummy2", "val2");

        startWorker();

        worker.cancel(true);

        assertEquals(1, metastorage.cache.size());
        assertEquals(INITIAL_VERSION, metastorage.read(versionKey()));
    }

    /** */
    @Test
    public void testUpdateSimple() throws Exception {
        startWorker();

        write("key1", "val1");

        stopWorker();

        // ver, val, hist.
        assertEquals(3, metastorage.cache.size());

        assertEquals(INITIAL_VERSION.nextVersion(histItem("key1", "val1")), metastorage.read(versionKey()));
        assertEquals("val1", metastorage.read(localKey("key1")));
        assertNotNull(metastorage.read(historyItemKey(1)));
    }

    /** */
    @Test
    public void testUpdateComplex() throws Exception {
        startWorker();

        write("key1", "val1");
        write("key2", "val2");
        write("key1", "val3");

        stopWorker();

        // ver, 2*val, 3*hist.
        assertEquals(6, metastorage.cache.size());

        DistributedMetaStorageVersion ver = (DistributedMetaStorageVersion)metastorage.read(versionKey());
        assertEquals(3, ver.id());

        assertEquals("val3", metastorage.read(localKey("key1")));
        assertEquals("val2", metastorage.read(localKey("key2")));

        assertNotNull(metastorage.read(historyItemKey(1)));
        assertNotNull(metastorage.read(historyItemKey(2)));
        assertNotNull(metastorage.read(historyItemKey(3)));
    }

    /** */
    @Test
    public void testRemoveHistoryItem() throws Exception {
        startWorker();

        write("key1", "val1");
        worker.removeHistItem(1);

        stopWorker();

        // ver, val.
        assertEquals(2, metastorage.cache.size());
        assertNull(metastorage.read(historyItemKey(1)));
    }

    /** */
    @Test
    public void testUpdateThenStart() throws Exception {
        write("key1", "val1");

        startWorker();
        stopWorker();

        // ver, val, hist.
        assertEquals(3, metastorage.cache.size());
    }

    /** */
    @Test
    public void testUpdateAfterStop() throws Exception {
        startWorker();
        stopWorker();

        write("key1", "val1");

        // ver.
        assertEquals(1, metastorage.cache.size());
    }

    /** */
    @Test
    public void testUpdateFullNodeData() throws Exception {
        write("key1", "val1");
        write("key2", "val2");

        // Write some crap.
        startWorker();
        stopWorker();

        startWorker();

        DistributedMetaStorageHistoryItem update = histItem("key3", "val3");

        DistributedMetaStorageVersion ver = INITIAL_VERSION.nextVersion(update);

        worker.update(new DistributedMetaStorageClusterNodeData(
            ver,
            new DistributedMetaStorageKeyValuePair[] {toKeyValuePair(update)},
            new DistributedMetaStorageHistoryItem[] {update},
            new DistributedMetaStorageHistoryItem[] {histItem("key4", "val4")} // Has to be ignored.
        ));

        stopWorker();

        // ver, val, hist.
        assertEquals(3, metastorage.cache.size());

        assertEquals(ver, metastorage.read(versionKey()));
        assertEquals("val3", metastorage.read(localKey("key3")));
        assertNotNull(metastorage.read(historyItemKey(1)));
    }

    /** */
    @Test
    public void testRestore1() throws Exception {
        metastorage.write(versionKey(), INITIAL_VERSION);

        DistributedMetaStorageHistoryItem histItem = histItem("key1", "val1");

        metastorage.write(historyItemKey(1), histItem);

        startWorker();
        stopWorker();

        // ver, val, hist.
        assertEquals(3, metastorage.cache.size());

        assertEquals(INITIAL_VERSION.nextVersion(histItem), metastorage.read(versionKey()));
        assertEquals(histItem, metastorage.read(historyItemKey(1)));
        assertEquals("val1", metastorage.read(localKey("key1")));
    }

    /** */
    @Test
    public void testRestore2() throws Exception {
        metastorage.write(versionKey(), INITIAL_VERSION);

        DistributedMetaStorageHistoryItem histItem = histItem("key1", "val1");

        metastorage.write(historyItemKey(1), histItem);
        metastorage.write(versionKey(), INITIAL_VERSION.nextVersion(histItem));

        startWorker();
        stopWorker();

        // ver, val, hist.
        assertEquals(3, metastorage.cache.size());

        assertEquals(INITIAL_VERSION.nextVersion(histItem), metastorage.read(versionKey()));
        assertEquals(histItem, metastorage.read(historyItemKey(1)));
        assertEquals("val1", metastorage.read(localKey("key1")));
    }

    /** */
    @Test
    public void testRestore3() throws Exception {
        metastorage.write(versionKey(), INITIAL_VERSION);

        DistributedMetaStorageHistoryItem histItem = histItem("key1", "val1");

        metastorage.write(historyItemKey(1), histItem);
        metastorage.write(versionKey(), INITIAL_VERSION.nextVersion(histItem));
        metastorage.write(localKey("key1"), "wrongValue");

        startWorker();
        stopWorker();

        // ver, val, hist.
        assertEquals(3, metastorage.cache.size());

        assertEquals(INITIAL_VERSION.nextVersion(histItem), metastorage.read(versionKey()));
        assertEquals(histItem, metastorage.read(historyItemKey(1)));
        assertEquals("val1", metastorage.read(localKey("key1")));
    }

    /** */
    @Test
    public void testHalt() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean await = new AtomicBoolean(true);

        metastorage = new MyReadWriteMetaStorageMock() {
            @Override public void writeRaw(String key, byte[] data) {
                try {
                    if (await.get())
                        latch.countDown();

                    while (worker.status() != DmsWorkerStatus.HALT) {
                        //noinspection BusyWait
                        Thread.sleep(0);
                    }
                }
                catch (Exception ignore) {
                }

                await.set(false);

                super.writeRaw(key, data);
            }
        };

        worker.setMetaStorage(metastorage);

        startWorker();

        worker.update(histItem("key1", "val1"));
        worker.update(histItem("key2", "val2"));

        latch.await();

        worker.cancel(true);

        // ver, val, hist.
        assertEquals(3, metastorage.cache.size());

        DistributedMetaStorageHistoryItem histItem = histItem("key1", "val1");

        assertEquals(INITIAL_VERSION.nextVersion(histItem), metastorage.read(versionKey()));
        assertEquals(histItem, metastorage.read(historyItemKey(1)));
        assertEquals("val1", metastorage.read(localKey("key1")));
    }

    /** */
    private DistributedMetaStorageKeyValuePair toKeyValuePair(DistributedMetaStorageHistoryItem histItem) {
        assertEquals(1, histItem.keys().length);

        return new DistributedMetaStorageKeyValuePair(histItem.keys()[0], histItem.valuesBytesArray()[0]);
    }

    /** */
    private void write(String key, String val) throws IgniteCheckedException {
        worker.update(histItem(key, val));
    }

    /** */
    private DistributedMetaStorageHistoryItem histItem(String key, String val) throws IgniteCheckedException {
        return new DistributedMetaStorageHistoryItem(key, JdkMarshaller.DEFAULT.marshal(val));
    }

    /** */
    private IgniteThread startWorker() throws InterruptedException {
        IgniteThread workerThread = new IgniteThread(worker);

        workerThread.start();

        while (workerThread.isAlive() && worker.runner() == null) {
            //noinspection BusyWait
            Thread.sleep(0);
        }

        return workerThread;
    }

    /** */
    private void stopWorker() throws InterruptedException {
        worker.cancel(false);
    }

    /** */
    private static class MockDmsLocalMetaStorageLock implements DmsLocalMetaStorageLock {
        /** */
        public final AtomicInteger lockCnt = new AtomicInteger();

        /** {@inheritDoc} */
        @Override public void lock() {
            lockCnt.incrementAndGet();
        }

        /** {@inheritDoc} */
        @Override public void unlock() {
            lockCnt.decrementAndGet();
        }
    }

    /** */
    private class MyReadWriteMetaStorageMock extends ReadWriteMetaStorageMock {
        /** {@inheritDoc} */
        @Override protected void assertLockIsHeldByWorkerThread() {
            Assert.assertTrue(Thread.currentThread() == testThread || lock.lockCnt.get() > 0);
        }
    }
}
