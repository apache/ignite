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

package org.apache.ignite.util;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.tree.SearchRow;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.util.lang.GridIterator;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.INDEX_FILE_NAME;

/**
 *
 */
public class GridCommandHandlerIndexingTest extends GridCommandHandlerTest {
    /** Test cache name. */
    private static final String CACHE_NAME = "persons-cache-vi";

    /**
     * Tests that validation doesn't fail if nothing is broken.
     */
    public void testValidateIndexesNoErrors() throws Exception {
        prepareGridForTest();

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "validate_indexes", CACHE_NAME));

        assertTrue(testOut.toString().contains("no issues found"));
    }

    /**
     * Tests that missing rows in CacheDataTree are detected.
     */
    public void testBrokenCacheDataTreeShouldFailValidation() throws Exception {
        Ignite ignite = prepareGridForTest();

        breakCacheDataTree(ignite, CACHE_NAME, 1);

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK,
            execute(
                "--cache",
                "validate_indexes",
                CACHE_NAME,
                "checkFirst", "10000",
                "checkThrough", "10"));

        assertTrue(testOut.toString().contains("issues found (listed above)"));

        assertTrue(testOut.toString().contains(
            "Key is present in SQL index, but is missing in corresponding data page."));
    }

    /**
     * Tests that missing rows in H2 indexes are detected.
     */
    public void testBrokenSqlIndexShouldFailValidation() throws Exception {
        Ignite ignite = prepareGridForTest();

        breakSqlIndex(ignite, CACHE_NAME);

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "validate_indexes", CACHE_NAME));

        assertTrue(testOut.toString().contains("issues found (listed above)"));
    }

    /**
     * Tests that missing rows in H2 indexes are detected.
     */
    public void testCorruptedIndexPartitionShouldFailValidation() throws Exception {
        Ignite ignite = prepareGridForTest();

        forceCheckpoint();

        File idxPath = indexPartition(ignite, CACHE_NAME);

        stopAllGrids();

        corruptIndexPartition(idxPath);

        startGrids(2);

        awaitPartitionMapExchange();

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "validate_indexes", CACHE_NAME));

        assertTrue(testOut.toString().contains("issues found (listed above)"));
    }

    /**
     *
     */
    private Ignite prepareGridForTest() throws Exception {
        Ignite ignite = startGrids(2);

        ignite.cluster().active(true);

        Ignite client = startGrid("client");

        String cacheName = "persons-cache-vi";

        client.getOrCreateCache(new CacheConfiguration<Integer, Person>()
            .setName(cacheName)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setBackups(1)
            .setQueryEntities(F.asList(personEntity(true, true)))
            .setAffinity(new RendezvousAffinityFunction(false, 32)));

        ThreadLocalRandom rand = ThreadLocalRandom.current();

        try (IgniteDataStreamer<Integer, Person> streamer = client.dataStreamer(CACHE_NAME);) {
            for (int i = 0; i < 10_000; i++)
                streamer.addData(i, new Person(rand.nextInt(), String.valueOf(rand.nextLong())));
        }

        return ignite;
    }

    /**
     * Get index partition file for specific node and cache.
     */
    private File indexPartition(Ignite ig, String cacheName) {
        IgniteEx ig0 = (IgniteEx)ig;

        FilePageStoreManager pageStoreManager = ((FilePageStoreManager)ig0.context().cache().context().pageStore());

        return new File(pageStoreManager.cacheWorkDir(false, cacheName), INDEX_FILE_NAME);
    }

    /**
     * Write some random trash in index partition.
     */
    private void corruptIndexPartition(File path) throws IOException {
        assertTrue(path.exists());

        ThreadLocalRandom rand = ThreadLocalRandom.current();

        try (RandomAccessFile idx = new RandomAccessFile(path, "rw")) {
            byte[] trash = new byte[1024];

            rand.nextBytes(trash);

            idx.seek(4096);

            idx.write(trash);
        }
    }

    /**
     * Removes some entries from a partition skipping index update. This effectively breaks the index.
     */
    private void breakCacheDataTree(Ignite ig, String cacheName, int partId) {
        IgniteEx ig0 = (IgniteEx)ig;
        int cacheId = CU.cacheId(cacheName);

        ScanQuery scanQry = new ScanQuery(partId);

        GridCacheContext<Object, Object> ctx = ig0.context().cache().context().cacheContext(cacheId);

        // Get current update counter
        String grpName = ig0.context().cache().context().cacheContext(cacheId).config().getGroupName();
        int cacheGrpId = grpName == null ? cacheName.hashCode() : grpName.hashCode();

        GridDhtLocalPartition locPart = ctx.dht().topology().localPartition(partId);
        IgniteCacheOffheapManager.CacheDataStore dataStore = ig0.context().cache().context().cache().cacheGroup(cacheGrpId).offheap().dataStore(locPart);

        Iterator<Cache.Entry> it = ig.cache(cacheName).withKeepBinary().query(scanQry).iterator();

        for (int i = 0; i < 5_000; i++) {
            if (it.hasNext()) {
                Cache.Entry entry = it.next();

                if (i % 5 == 0) {
                    // Do update
                    GridCacheDatabaseSharedManager db = (GridCacheDatabaseSharedManager)ig0.context().cache().context().database();

                    db.checkpointReadLock();

                    try {
                        IgniteCacheOffheapManager.CacheDataStore innerStore = U.field(dataStore, "delegate");

                        // IgniteCacheOffheapManagerImpl.CacheDataRowStore
                        Object rowStore = U.field(innerStore, "rowStore");

                        // IgniteCacheOffheapManagerImpl.CacheDataTree
                        Object dataTree = U.field(innerStore, "dataTree");

                        CacheDataRow oldRow = U.invoke(
                            dataTree.getClass(),
                            dataTree,
                            "remove",
                            new SearchRow(cacheId, ctx.toCacheKeyObject(entry.getKey())));

                        if (oldRow != null)
                            U.invoke(rowStore.getClass(), rowStore, "removeRow", oldRow.link());
                    }
                    catch (IgniteCheckedException e) {
                        System.out.println("Failed to remove key skipping indexes: " + entry);

                        e.printStackTrace();
                    }
                    finally {
                        db.checkpointReadUnlock();
                    }
                }
            }
            else {
                System.out.println("Early exit for index corruption, keys processed: " + i);

                break;
            }
        }
    }

    /**
     * Removes some entries from H2 trees skipping partition updates. This effectively breaks the index.
     */
    private void breakSqlIndex(Ignite ig, String cacheName) throws Exception {
        GridQueryProcessor qry = ((IgniteEx)ig).context().query();

        GridCacheContext<Object, Object> ctx = ((IgniteEx)ig).cachex(cacheName).context();

        GridDhtLocalPartition locPart = ctx.topology().localPartitions().get(0);

        GridIterator<CacheDataRow> it = ctx.group().offheap().partitionIterator(locPart.id());

        for (int i = 0; i < 500; i++) {
            if (!it.hasNextX()) {
                System.out.println("Early exit for index corruption, keys processed: " + i);

                break;
            }

            CacheDataRow row = it.nextX();

            ctx.shared().database().checkpointReadLock();

            try {
                qry.remove(ctx, row);
            }
            finally {
                ctx.shared().database().checkpointReadUnlock();
            }
        }
    }

    /**
     * @param idxName Index name.
     * @param idxOrgId Index org id.
     */
    private QueryEntity personEntity(boolean idxName, boolean idxOrgId) {
        QueryEntity entity = new QueryEntity();

        entity.setKeyType(Integer.class.getName());
        entity.setValueType(Person.class.getName());

        entity.addQueryField("orgId", Integer.class.getName(), null);
        entity.addQueryField("name", String.class.getName(), null);

        List<QueryIndex> idxs = new ArrayList<>();

        if (idxName) {
            QueryIndex idx = new QueryIndex("name");

            idxs.add(idx);
        }

        if (idxOrgId) {
            QueryIndex idx = new QueryIndex("orgId");

            idxs.add(idx);
        }

        entity.setIndexes(idxs);

        return entity;
    }

    /**
     *
     */
    private static class Person implements Serializable {
        /** */
        int orgId;

        /** */
        String name;

        /**
         * @param orgId Organization ID.
         * @param name Name.
         */
        public Person(int orgId, String name) {
            this.orgId = orgId;
            this.name = name;
        }
    }
}
