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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
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
import org.apache.ignite.internal.processors.cache.tree.SearchRow;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.util.lang.GridIterator;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;

/**
 *
 */
public class GridCommandHandlerIndexingTest extends GridCommandHandlerTest {
    /**
     * Tests that validation doesn't fail if nothing is broken.
     */
    public void testValidateIndexesNoErrors() throws Exception {
        Ignite ignite = startGrids(2);

        ignite.cluster().active(true);

        Ignite client = startGrid("client");

        String cacheName = "persons-cache-vi";

        IgniteCache<Integer, Person> personCache = createPersonCache(client, cacheName);

        ThreadLocalRandom rand = ThreadLocalRandom.current();

        for (int i = 0; i < 10_000; i++)
            personCache.put(i, new Person(rand.nextInt(), String.valueOf(rand.nextLong())));

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "validate_indexes", cacheName));

        assertTrue(testOut.toString().contains("validate_indexes has finished, no issues found"));
    }

    /**
     * Tests that missing rows in CacheDataTree are detected.
     */
    public void testBrokenCacheDataTreeShouldFailValidation() throws Exception {
        Ignite ignite = startGrids(2);

        ignite.cluster().active(true);

        Ignite client = startGrid("client");

        String cacheName = "persons-cache-vi";

        IgniteCache<Integer, Person> personCache = createPersonCache(client, cacheName);

        ThreadLocalRandom rand = ThreadLocalRandom.current();

        for (int i = 0; i < 10_000; i++)
            personCache.put(i, new Person(rand.nextInt(), String.valueOf(rand.nextLong())));

        breakCacheDataTree(ignite, cacheName, 1);

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK,
            execute(
                "--cache",
                "validate_indexes",
                cacheName,
                "checkFirst", "10000",
                "checkThrough", "10"));

        assertTrue(testOut.toString().contains("validate_indexes has finished with errors"));

        assertTrue(testOut.toString().contains(
            "Key is present in SQL index, but is missing in corresponding data page."));
    }

    /**
     * Tests that missing rows in H2 indexes are detected.
     */
    public void testBrokenSqlIndexShouldFailValidation() throws Exception {
        Ignite ignite = startGrids(2);

        ignite.cluster().active(true);

        Ignite client = startGrid("client");

        String cacheName = "persons-cache-vi";

        IgniteCache<Integer, Person> personCache = createPersonCache(client, cacheName);

        ThreadLocalRandom rand = ThreadLocalRandom.current();

        for (int i = 0; i < 10_000; i++)
            personCache.put(i, new Person(rand.nextInt(), String.valueOf(rand.nextLong())));

        breakSqlIndex(ignite, cacheName);

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "validate_indexes", cacheName));

        assertTrue(testOut.toString().contains("validate_indexes has finished with errors"));
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
     * Dynamically creates cache with SQL indexes.
     *
     * @param ig Client.
     * @param cacheName Cache name.
     */
    private IgniteCache<Integer, Person> createPersonCache(Ignite ig, String cacheName) {
        return ig.getOrCreateCache(new CacheConfiguration<Integer, Person>()
            .setName(cacheName)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setBackups(1)
            .setQueryEntities(F.asList(personEntity(true, true)))
            .setAffinity(new RendezvousAffinityFunction(false, 32)));
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
