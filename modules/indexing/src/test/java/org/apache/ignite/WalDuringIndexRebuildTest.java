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

package org.apache.ignite;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory.IteratorParametersBuilder;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import static org.apache.ignite.WalDuringIndexRebuildTest.RebuildType.REMOVE_INDEX_FILE;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.configuration.DataStorageConfiguration.UNLIMITED_WAL_ARCHIVE;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.BTREE_PAGE_INSERT;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.cacheId;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;
import static org.apache.ignite.internal.util.IgniteUtils.MB;

/** */
@RunWith(Parameterized.class)
@SuppressWarnings({"resource"})
public class WalDuringIndexRebuildTest extends GridCommonAbstractTest {
    /** Batches count. */
    public static final int ENTRIES_CNT = 10_000;

    /** Rebuild type. */
    @Parameter
    public RebuildType rebuildType;

    /** */
    @Parameters(name = "rebuildType={0}")
    public static List<Object[]> parameters() {
        List<Object[]> params = new ArrayList<>();

        for (RebuildType rebuildType : RebuildType.values())
            params.add(new Object[]{rebuildType});

        return params;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setConsistentId(igniteInstanceName)
            .setClusterStateOnStart(INACTIVE)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true))
                .setMaxWalArchiveSize(UNLIMITED_WAL_ARCHIVE));
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

        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testIndexRebuild() throws Exception {
        IgniteEx srv = startGrid(0);

        srv.cluster().state(ACTIVE);

        createAndPopulateCache(srv, DEFAULT_CACHE_NAME);

        forceCheckpoint(srv);

        WALPointer walPrtBefore = srv.context()
                .cache()
                .context()
                .wal()
                .lastWritePointer();

        long walIdxBefore = curWalIdx(srv);

        String dirName = srv.name().replace(".", "_");

        Path idxFileBefore = checkIdxFileExists(dirName);

        long startTs;

        if (rebuildType == REMOVE_INDEX_FILE) {
            srv.cluster().state(INACTIVE);

            awaitPartitionMapExchange();

            stopGrid(0);

            Files.delete(idxFileBefore);

            srv = startGrid(0);

            startTs = System.currentTimeMillis();

            srv.cluster().state(ACTIVE);
        }
        else {
            startTs = System.currentTimeMillis();

            forceRebuildIndexes(srv, srv.cachex(DEFAULT_CACHE_NAME).context());
        }

        indexRebuildFuture(srv, cacheId(DEFAULT_CACHE_NAME)).get(getTestTimeout());

        long finishTs = System.currentTimeMillis();

        log.warning(">>>>>> Index rebuild finished and took " + (finishTs - startTs) + " ms");

        forceCheckpoint(srv);

        long walIdxAfter = curWalIdx(srv);

        log.warning(">>>>>> Index rebuild generated " + (walIdxAfter - walIdxBefore) + " segments");

        Map<RecordType, Long> recTypesBefore = countWalRecordsByTypes(dirName,
            (rt, wp) -> wp.compareTo(walPrtBefore) <= 0);

        Map<RecordType, Long> recTypesAfter = countWalRecordsByTypes(dirName,
            (rt, wp) -> wp.compareTo(walPrtBefore) > 0);

        log.warning(">>>>>> WalRecords comparison:");
        log.warning(String.format("%-62.60s%-30.28s%-30.28s\n", "Record type", "Data load (before rebuild)",
                "After index rebuild"));

        for (RecordType recType : RecordType.values()) {
            log.warning(String.format("%-62.60s%-30.28s%-30.28s",
                recType,
                recTypesBefore.get(recType),
                recTypesAfter.get(recType)));
        }

        checkIdxFileExists(dirName);

        assertEquals((Long)0L, recTypesAfter.getOrDefault(BTREE_PAGE_INSERT, 0L));
    }

    /** @param ignite Ignite. */
    private long curWalIdx(IgniteEx ignite) {
        return ignite.context()
            .cache()
            .context()
            .wal()
            .currentSegment();
    }

    /** */
    private void createAndPopulateCache(IgniteEx ignite, String cacheName) {
        int fieldsCnt = 2;

        LinkedHashMap<String, String> fields = new LinkedHashMap<>(fieldsCnt);

        List<QueryIndex> indexes = new ArrayList<>(fieldsCnt);

        for (int i = 0; i < fieldsCnt; i++) {
            String fieldName = "F" + i;

            fields.put(fieldName, String.class.getName());

            indexes.add(new QueryIndex(fieldName).setInlineSize(128));
        }

        QueryEntity qryEntity = new QueryEntity()
            .setKeyType(Integer.class.getName())
            .setValueType("TestVal")
            .setFields(fields)
            .setIndexes(indexes);

        IgniteCache<Integer, BinaryObject> cache = ignite.getOrCreateCache(
            new CacheConfiguration<>(cacheName)
                .setQueryEntities(Collections.singleton(qryEntity)))
            .withKeepBinary();

        BinaryObjectBuilder binObjBuilder = ignite.binary().builder("TestVal");

        for (String field : fields.keySet())
            binObjBuilder.setField(field, UUID.randomUUID().toString());

        BinaryObject binObj = binObjBuilder.build();

        for (int i = 0; i < ENTRIES_CNT; i++)
            cache.put(i, binObj);

        log.warning(">>>>>> Puts finished.");

        assertTrue("Unexpected indexes count", ignite.context().indexProcessor()
            .indexes(cacheName).size() >= fieldsCnt);
    }

    /**
     * @param dirName Directory name.
     */
    private Path checkIdxFileExists(String dirName) throws IgniteCheckedException, IOException {
        Path pdsDir = U.resolveWorkDirectory(
            U.defaultWorkDirectory(),
            DFLT_STORE_DIR + File.separatorChar + dirName + File.separatorChar + "cache-" + DEFAULT_CACHE_NAME,
            false
        ).toPath();

        Path idxFile = Files.list(pdsDir)
            .filter(p -> p.endsWith("index.bin"))
            .findFirst()
            .orElseThrow(() -> new IgniteException("index.bin not found"));

        assertTrue("Index file not found", idxFile.toFile().exists());

        log.warning(">>>>>> Index file size: " + Files.size(idxFile) / MB + " MB");

        return idxFile;
    }

    /**
     * @param dn2DirName Node directory name.
     * @param predicate Predicate.
     */
    private Map<RecordType, Long> countWalRecordsByTypes(String dn2DirName,
        IgniteBiPredicate<RecordType, WALPointer> predicate) throws IgniteCheckedException {

        File walDir = U.resolveWorkDirectory(U.defaultWorkDirectory(),
            DFLT_STORE_DIR + "/wal/" + dn2DirName, false);

        File walArchiveDir = U.resolveWorkDirectory(U.defaultWorkDirectory(),
            DFLT_STORE_DIR + "/wal/archive/" + dn2DirName, false);

        IgniteWalIteratorFactory factory = new IgniteWalIteratorFactory(log);

        IteratorParametersBuilder beforeIdxRemoveBldr = new IteratorParametersBuilder()
            .filesOrDirs(walDir, walArchiveDir)
            .filter(predicate);

        Map<RecordType, Long> cntByRecTypes = new EnumMap<>(RecordType.class);

        for (RecordType recType : RecordType.values())
            cntByRecTypes.put(recType, 0L);

        try (WALIterator walIter = factory.iterator(beforeIdxRemoveBldr)) {
            while (walIter.hasNext()) {
                IgniteBiTuple<WALPointer, WALRecord> entry = walIter.next();

                RecordType recType = entry.getValue().type();

                cntByRecTypes.merge(recType, 1L, Long::sum);
            }
        }

        return cntByRecTypes;
    }

    /** */
    public enum RebuildType {
        /** Remove index file. */
        REMOVE_INDEX_FILE,

        /** Force index rebuild. */
        FORCE_INDEX_REBUILD
    }
}
