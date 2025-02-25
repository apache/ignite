/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.OpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIODecorator;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree;
import org.apache.ignite.internal.processors.cache.persistence.filename.SnapshotFileTree;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_SNAPSHOT_SEQUENTIAL_WRITE;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.processors.cache.persistence.filename.SnapshotFileTree.partDeltaIndexFile;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.DeltaSortedIterator.DELTA_SORT_BATCH_SIZE;
import static org.junit.Assert.assertArrayEquals;

/**
 * Cluster snapshot delta tests.
 */
@RunWith(Parameterized.class)
public class IgniteClusterSnapshotDeltaTest extends AbstractSnapshotSelfTest {
    /** */
    @Parameterized.Parameter(2)
    public boolean sequentialWrite;

    /** Parameters. */
    @Parameterized.Parameters(name = "encryption={0}, onlyPrimary={1}, sequentialWrite={1}")
    public static Collection<Object[]> parameters() {
        Collection<Object[]> baseParams = params();

        List<Object[]> res = new ArrayList<>();

        for (boolean seqWrite : new boolean[] {false, true}) {
            for (Object[] baseParam : baseParams) {
                Object[] res0 = Arrays.copyOf(baseParam, baseParam.length + 1);

                res0[baseParam.length] = seqWrite;

                res.add(res0);
            }
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        System.clearProperty(IGNITE_SNAPSHOT_SEQUENTIAL_WRITE);
    }

    /** @throws Exception If failed. */
    @Test
    public void testSendDelta() throws Exception {
        int keys = 10_000;
        byte[] payload = new byte[PAGE_SIZE / 2];
        int partCnt = 2;

        System.setProperty(IGNITE_SNAPSHOT_SEQUENTIAL_WRITE, String.valueOf(sequentialWrite));

        // 1. Start a cluster and fill cache.
        ThreadLocalRandom.current().nextBytes(payload);

        byte[] expPayload = Arrays.copyOf(payload, payload.length);

        CacheConfiguration<Integer, byte[]> ccfg = new CacheConfiguration<Integer, byte[]>(DEFAULT_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction(false, partCnt));

        String cacheDir = NodeFileTree.cacheDirName(false, DEFAULT_CACHE_NAME);

        IgniteEx srv = startGridsWithCache(1, keys, (k) -> expPayload, ccfg);

        if (sequentialWrite)
            injectSequentialWriteCheck(srv);

        IgniteSnapshotManager mgr = snp(srv);

        Function<SnapshotFileTree, SnapshotSender> old = mgr.localSnapshotSenderFactory();

        CountDownLatch partStart = new CountDownLatch(partCnt);
        CountDownLatch deltaApply = new CountDownLatch(1);

        mgr.localSnapshotSenderFactory(sft -> new DelegateSnapshotSender(log,
            mgr.snapshotExecutorService(), old.apply(sft)) {
            @Override public void sendPart0(File part, File snpCacheDir, GroupPartitionId pair, Long length) {
                if (cacheDir.equals(snpCacheDir.getName()))
                    partStart.countDown();

                try {
                    deltaApply.await();
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }

                super.sendPart0(part, snpCacheDir, pair, length);
            }

            @Override public void sendDelta0(File delta, String cacheDirName, GroupPartitionId pair) {
                if (cacheDir.equals(cacheDirName) && pair.getPartitionId() != INDEX_PARTITION)
                    assertTrue("Delta length : " + delta.length() + " > 0", delta.length() > 0);

                if (!sequentialWrite)
                    U.delete(partDeltaIndexFile(delta));

                long start = System.nanoTime();

                super.sendDelta0(delta, cacheDirName, pair);

                if (cacheDir.equals(cacheDirName)) {
                    log.info("Send delta [size=" + U.humanReadableByteCount(delta.length()) +
                        ", time=" + (U.nanosToMillis(System.nanoTime() - start)) + "ms, part=" + pair + "]");
                }
            }
        });

        // 2. Start a snapshot and block copy of a partitions.
        IgniteFuture<Void> fut = snp(srv).createSnapshot(SNAPSHOT_NAME, null, false, onlyPrimary);

        GridTestUtils.waitForCondition(() -> mgr.currentCreateRequest() != null, getTestTimeout());

        partStart.await();

        // 3. Produce delta pages by data updates.
        IgniteCache<Integer, byte[]> cache = srv.getOrCreateCache(ccfg);

        ThreadLocalRandom.current().nextBytes(payload);

        for (int i = 0; i < keys; i++)
            cache.put(i, payload);

        forceCheckpoint(srv);

        // 4. Apply delta and wait for a snapshot complete.
        deltaApply.countDown();

        fut.get();

        // 5. Destroy cache, restart the cluster and check data (delta was successfully applied).
        srv.destroyCache(DEFAULT_CACHE_NAME);

        stopAllGrids();

        srv = startGridsFromSnapshot(1, SNAPSHOT_NAME);

        cache = srv.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < keys; i++)
            assertArrayEquals(expPayload, cache.get(i));
    }

    /** Injects test IO that checks sequential write to a pagestore on a delta apply. */
    private void injectSequentialWriteCheck(IgniteEx srv) {
        FilePageStoreManager pageStore = (FilePageStoreManager)srv.context().cache().context().pageStore();

        FileIOFactory old = pageStore.getPageStoreFileIoFactory();

        int idxsPerPage = pageStore.pageSize() / 4;

        int idxsPerBatch = (DELTA_SORT_BATCH_SIZE / idxsPerPage) * idxsPerPage + idxsPerPage;

        FileIOFactory testFactory = new FileIOFactory() {
            @Override public FileIO create(File file, OpenOption... modes) throws IOException {
                FileIO fileIo = old.create(file, modes);

                return new FileIODecorator(fileIo) {
                    boolean isSequentialWrite = true;

                    long lastPos;

                    int idx;

                    @Override public int write(ByteBuffer srcBuf, long pos) throws IOException {
                        boolean batchRotation = idx++ % idxsPerBatch == 0;

                        if (lastPos > pos && !batchRotation)
                            isSequentialWrite = false;

                        lastPos = pos;

                        return super.write(srcBuf, pos);
                    }

                    @Override public void close() throws IOException {
                        super.close();

                        if (!isSequentialWrite)
                            throw new RuntimeException("Non sequential write.");
                    }
                };
            }
        };

        pageStore.setPageStoreFileIOFactories(testFactory, testFactory);
    }
}
