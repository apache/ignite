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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.ClusterSnapshotRecord;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.spi.systemview.view.SnapshotView;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.spi.systemview.view.SnapshotView.SNAPSHOT_SYS_VIEW;

/** */
public class IgniteClusterSnapshotWalRecordTest extends AbstractSnapshotSelfTest {
    /**
     * Tests that cluster snapshot contains only data written to WAL before {@link ClusterSnapshotRecord}.
     */
    @Test
    public void testClusterSnapshotRecordCorrectlySplitsWAL() throws Exception {
        IgniteEx ign = startGridsWithCache(1, CACHE_KEYS_RANGE, key -> new Account(key, key),
            new CacheConfiguration<>(DEFAULT_CACHE_NAME));

        CountDownLatch loadStopLatch = new CountDownLatch(1);

        // Start changing data concurrently with performing the ClusterSnapshot operation.
        IgniteInternalFuture<?> loadFut = null;

        try {
            loadFut = GridTestUtils.runMultiThreadedAsync(() -> {
                Random r = new Random();

                while (loadStopLatch.getCount() > 0 && !Thread.interrupted()) {
                    int key = r.nextInt(CACHE_KEYS_RANGE);

                    Account acc = new Account(r.nextInt(), r.nextInt());

                    ign.cache(DEFAULT_CACHE_NAME).put(key, acc);
                }
            }, 5, "cache-loader-");

            snp(ign).createSnapshot(SNAPSHOT_NAME, onlyPrimary).get();

            checkSnapshot(SNAPSHOT_NAME);

            loadStopLatch.countDown();

            loadFut.get();
        }
        catch (Throwable err) {
            if (loadFut != null)
                loadFut.cancel();

            throw err;
        }

        T2<Map<Integer, Account>, Map<Integer, Account>> data = parseWalCacheState(ign, SNAPSHOT_NAME);

        Map<Integer, Account> snpData = data.get1();
        Map<Integer, Account> finalData = data.get2();

        assertCacheKeys(ign.cache(DEFAULT_CACHE_NAME), finalData);

        ign.destroyCache(DEFAULT_CACHE_NAME);
        ensureCacheAbsent(dfltCacheCfg);

        stopGrid(0);

        IgniteEx snpIgn = startGridsFromSnapshot(1, SNAPSHOT_NAME);

        assertCacheKeys(snpIgn.cache(DEFAULT_CACHE_NAME), snpData);
    }

    /** */
    @Test
    public void testClusterSnapshotRecordIsWrittenToSnapshotMetadata() throws Exception {
        int nodes = 3;
        int snapshots = 10;

        startGridsWithCache(nodes, 1, key -> new Account(key, key),
            new CacheConfiguration<>(DEFAULT_CACHE_NAME)
        );

        for (int i = 0; i < snapshots; i++) {
            // Start changing data concurrently with performing the ClusterSnapshot operation.
            snp(grid(0)).createSnapshot(SNAPSHOT_NAME + i, onlyPrimary).get();

            checkSnapshot(SNAPSHOT_NAME + i);
        }

        for (int i = 0; i < nodes; i++) {
            IgniteEx ign = grid(i);

            ign.context().cache().context().wal().flush(null, true);

            WALIterator walIt = wal(grid(i));

            long snpCnt = 0;

            SystemView<SnapshotView> snpView = ign.context().systemView().view(SNAPSHOT_SYS_VIEW);

            for (IgniteBiTuple<WALPointer, WALRecord> tuple: walIt) {
                WALRecord rec = tuple.getValue();

                if (rec.type() == WALRecord.RecordType.CLUSTER_SNAPSHOT) {
                    SnapshotMetadata metadata = snp(grid(i)).readSnapshotMetadata(
                        snp(grid(i)).snapshotLocalDir(SNAPSHOT_NAME + snpCnt),
                        (String)grid(i).configuration().getConsistentId());

                    assertEquals(tuple.getKey(), metadata.snapshotRecordPointer());

                    List<SnapshotView> snpNodesView = StreamSupport.stream(snpView.spliterator(), false)
                        .filter(v -> v.name().equals(metadata.snapshotName()))
                        .filter(v -> v.consistentId().equals(ign.localNode().consistentId()))
                        .filter(v -> v.snapshotRecordSegment().equals(tuple.getKey().index()))
                        .collect(Collectors.toList());

                    assertEquals(1, snpNodesView.size());

                    snpCnt++;
                }
            }

            assertEquals(snapshots, snpCnt);
        }
    }

    /**
     * Parsing WAL files and dumping cache states: fisrst is before {@link ClusterSnapshotRecord} was written, and second
     * is after all load operations stopped.
     *
     * @param ign Ignite instance.
     * @param snpName Cluster snapshot name.
     */
    private T2<Map<Integer, Account>, Map<Integer, Account>> parseWalCacheState(IgniteEx ign, String snpName) throws Exception {
        ign.context().cache().context().wal().flush(null, true);

        WALIterator walIt = wal(ign);

        assertTrue(walIt.hasNext());

        WALPointer snpRecPtr = null;

        Map<Integer, Account> snpData = new HashMap<>();
        Map<Integer, Account> finalData = new HashMap<>();

        CacheObjectContext cacheObjCtx = ign.cachex(DEFAULT_CACHE_NAME).context().cacheObjectContext();

        for (IgniteBiTuple<WALPointer, WALRecord> rec: walIt) {
            if (rec.getValue() instanceof ClusterSnapshotRecord) {
                assertEquals(snpName, ((ClusterSnapshotRecord)rec.getValue()).clusterSnapshotName());

                assertNull(snpRecPtr);

                snpRecPtr = rec.get1();
            }

            if (rec.getValue() instanceof DataRecord) {
                DataRecord data = (DataRecord)rec.getValue();

                assertEquals(1, data.writeEntries().size());

                DataEntry e = data.writeEntries().get(0);

                Integer key = e.key().value(cacheObjCtx, false);
                Account val = e.value().value(cacheObjCtx, false);

                if (snpRecPtr == null)
                    snpData.put(key, val);

                finalData.put(key, val);
            }
        }

        assertNotNull(snpRecPtr);
        assertFalse(F.isEmpty(snpData));
        assertFalse(F.isEmpty(finalData));
        assertFalse(snpData.equals(finalData));

        return new T2<>(snpData, finalData);
    }

    /**
     * @param ign Ignite instance.
     * @return WAL iterator over existing WAL files.
     */
    private WALIterator wal(IgniteEx ign) throws Exception {
        Path workPath = Paths.get(U.defaultWorkDirectory());

        IgniteWalIteratorFactory factory = new IgniteWalIteratorFactory(log);

        String subfolderName = U.maskForFileName(ign.localNode().consistentId().toString());

        File wals = workPath.resolve(DataStorageConfiguration.DFLT_WAL_PATH).resolve(subfolderName).toFile();
        File archive = workPath.resolve(DataStorageConfiguration.DFLT_WAL_ARCHIVE_PATH).resolve(subfolderName).toFile();

        IgniteWalIteratorFactory.IteratorParametersBuilder params = new IgniteWalIteratorFactory.IteratorParametersBuilder()
            .filesOrDirs(archive, wals)
            .sharedContext(ign.context().cache().context());

        return factory.iterator(params);
    }

    /**
     * @param cache Cache.
     * @param walCacheDump Cache dump built with parsing WAL.
     */
    private void assertCacheKeys(IgniteCache<Integer, Account> cache, Map<Integer, Account> walCacheDump) {
        cache.query(new ScanQuery<Integer, Account>(null))
            .forEach(e -> {
                Account dumpedVal = walCacheDump.remove(e.getKey());

                assertEquals(e.getValue(), dumpedVal);
            });

        assertTrue(walCacheDump.toString(), F.isEmpty(walCacheDump));
    }
}
