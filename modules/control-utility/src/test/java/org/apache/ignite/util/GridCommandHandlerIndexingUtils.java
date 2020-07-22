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
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import javax.cache.Cache.Entry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager.CacheDataStore;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.tree.CacheDataRowStore;
import org.apache.ignite.internal.processors.cache.tree.CacheDataTree;
import org.apache.ignite.internal.processors.cache.tree.SearchRow;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.util.lang.GridIterator;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.jetbrains.annotations.Nullable;

import static java.lang.String.valueOf;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.metric.IoStatisticsHolderNoOp.INSTANCE;
import static org.apache.ignite.internal.util.IgniteUtils.field;
import static org.apache.ignite.internal.util.IgniteUtils.hasField;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.CacheEntityThreeFields.DOUBLE_NAME;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.CacheEntityThreeFields.ID_NAME;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.CacheEntityThreeFields.STR_NAME;

/**
 * Utility class for tests.
 */
public class GridCommandHandlerIndexingUtils {
    /** Test cache name. */
    public static final String CACHE_NAME = "persons-cache-vi";

    /** Cache name second. */
    static final String CACHE_NAME_SECOND = CACHE_NAME + "-second";

    /** Test group name. */
    static final String GROUP_NAME = "group1";

    /** Test group name. */
    static final String GROUP_NAME_SECOND = GROUP_NAME + "_second";

    /** Three entries cache name common partition. */
    static final String THREE_ENTRIES_CACHE_NAME_COMMON_PART = "three_entries";

    /** Private constructor */
    private GridCommandHandlerIndexingUtils() {
        throw new IllegalArgumentException("don't create");
    }

    /**
     * Create and fill cache. Key - integer, value - {@link Person}.
     * {@link #createAndFillCache(Ignite, String, String, String, Map, int)}
     * is used with {@link #personEntity()}, default dataRegion and cnt=10_000.
     *
     * @param ignite Node.
     * @param cacheName Cache name.
     * @param grpName Group name.
     */
    public static void createAndFillCache(Ignite ignite, String cacheName, String grpName) {
        createAndFillCache(
            ignite,
            cacheName,
            grpName,
            null,
            singletonMap(personEntity(), rand -> new Person(rand.nextInt(), valueOf(rand.nextLong()))),
            10_000
        );
    }

    /**
     * Create and fill cache.
     * <br/>
     * <table class="doctable">
     * <th>Cache parameter</th>
     * <th>Value</th>
     * <tr>
     *     <td>Synchronization mode</td>
     *     <td>{@link CacheWriteSynchronizationMode#FULL_SYNC FULL_SYNC}</td>
     * </tr>
     * <tr>
     *     <td>Atomicity mode</td>
     *     <td>{@link CacheAtomicityMode#ATOMIC ATOMIC}</td>
     * </tr>
     * <tr>
     *     <td>Number of backup</td>
     *     <td>1</td>
     * </tr>
     * <tr>
     *     <td>Affinity</td>
     *     <td>{@link RendezvousAffinityFunction} with exclNeighbors = false, parts = 32</td>
     * </tr>
     * </table>
     *
     * @param ignite Node.
     * @param cacheName Cache name.
     * @param grpName Group name.
     * @param dataRegionName DataRegionConfiguration name, null for default.
     * @param qryEntities QueryEntities and functions for creating them.
     * @param cnt How many entities create for each {@code qryEntities}.
     */
    public static void createAndFillCache(
        Ignite ignite,
        String cacheName,
        String grpName,
        @Nullable String dataRegionName,
        Map<QueryEntity, Function<Random, Object>> qryEntities,
        int cnt
    ) {
        requireNonNull(ignite);
        requireNonNull(cacheName);
        requireNonNull(grpName);
        requireNonNull(qryEntities);

        ignite.createCache(new CacheConfiguration<>()
            .setName(cacheName)
            .setGroupName(grpName)
            .setDataRegionName(dataRegionName)
            .setWriteSynchronizationMode(FULL_SYNC)
            .setAtomicityMode(ATOMIC)
            .setBackups(1)
            .setQueryEntities(new ArrayList<>(qryEntities.keySet()))
            .setAffinity(new RendezvousAffinityFunction(false, 32)));

        ThreadLocalRandom rand = ThreadLocalRandom.current();

        try (IgniteDataStreamer<Integer, Object> streamer = ignite.dataStreamer(cacheName)) {
            int entity = 0;
            for (Function<Random, Object> fun : qryEntities.values()) {
                for (int i = 0; i < cnt; i++)
                    streamer.addData(i + (entity * cnt), fun.apply(rand));

                streamer.flush();
                entity++;
            }
        }
    }

    /**
     * Deleting a rows from the cache without updating indexes.
     *
     * @param log Logger.
     * @param internalCache Cache.
     * @param partId Partition number.
     * @param filter Entry filter.
     */
    static <K, V> void breakCacheDataTree(
        IgniteLogger log,
        IgniteInternalCache<K, V> internalCache,
        int partId,
        @Nullable BiPredicate<Integer, Entry<K, V>> filter
    ) {
        requireNonNull(log);
        requireNonNull(internalCache);

        GridCacheContext<K, V> cacheCtx = internalCache.context();

        GridDhtLocalPartition dhtLocPart = cacheCtx.dht().topology().localPartition(partId);

        CacheDataStore cacheDataStore = cacheCtx.group().offheap().dataStore(dhtLocPart);

        String delegate = "delegate";
        if (hasField(cacheDataStore, delegate))
            cacheDataStore = field(cacheDataStore, delegate);

        CacheDataRowStore cacheDataRowStore = field(cacheDataStore, "rowStore");
        CacheDataTree cacheDataTree = field(cacheDataStore, "dataTree");

        String cacheName = internalCache.name();

        QueryCursor<Entry<K, V>> qryCursor = cacheCtx.kernalContext().grid().cache(cacheName).withKeepBinary()
            .query(new ScanQuery<>(partId));

        Iterator<Entry<K, V>> cacheEntryIter = qryCursor.iterator();

        IgniteCacheDatabaseSharedManager db = cacheCtx.shared().database();
        int cacheId = CU.cacheId(cacheName);
        int i = 0;

        while (cacheEntryIter.hasNext()) {
            Entry<K, V> entry = cacheEntryIter.next();

            if (nonNull(filter) && !filter.test(i++, entry))
                continue;

            db.checkpointReadLock();

            try {
                CacheDataRow oldRow = cacheDataTree.remove(
                    new SearchRow(cacheId, cacheCtx.toCacheKeyObject(entry.getKey()))
                );

                if (nonNull(oldRow))
                    cacheDataRowStore.removeRow(oldRow.link(), INSTANCE);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to remove key skipping indexes: " + entry, e);
            }
            finally {
                db.checkpointReadUnlock();
            }
        }
    }

    /**
     * Deleting records from the index bypassing cache.
     *
     * @param internalCache Cache.
     * @param partId Partition number.
     * @param filter Row filter.
     * @throws Exception If failed.
     */
    static <K, V> void breakSqlIndex(
        IgniteInternalCache<K, V> internalCache,
        int partId,
        @Nullable Predicate<CacheDataRow> filter
    ) throws Exception {
        requireNonNull(internalCache);

        GridCacheContext<K, V> cacheCtx = internalCache.context();

        GridDhtLocalPartition locPart = cacheCtx.topology().localPartitions().get(partId);
        GridIterator<CacheDataRow> cacheDataGridIter = cacheCtx.group().offheap().partitionIterator(locPart.id());

        GridQueryProcessor qryProcessor = internalCache.context().kernalContext().query();

        while (cacheDataGridIter.hasNextX()) {
            CacheDataRow cacheDataRow = cacheDataGridIter.nextX();

            if (nonNull(filter) && !filter.test(cacheDataRow))
                continue;

            cacheCtx.shared().database().checkpointReadLock();

            try {
                qryProcessor.remove(cacheCtx, cacheDataRow);
            }
            finally {
                cacheCtx.shared().database().checkpointReadUnlock();
            }
        }
    }

    /**
     * Creates and fills cache.
     *
     * @param ignite Ignite instance.
     * @param cacheName Cache name.
     * @param grpName Cache group.
     * @param entities Collection of {@link QueryEntity}.
     */
    static void createAndFillThreeFieldsEntryCache(
        final Ignite ignite,
        final String cacheName,
        final String grpName,
        final Collection<QueryEntity> entities)
    {
        assert nonNull(ignite);
        assert nonNull(cacheName);

        ignite.createCache(new CacheConfiguration<Integer, CacheEntityThreeFields>()
            .setName(cacheName)
            .setGroupName(grpName)
            .setWriteSynchronizationMode(FULL_SYNC)
            .setAtomicityMode(ATOMIC)
            .setBackups(1)
            .setQueryEntities(entities)
            .setAffinity(new RendezvousAffinityFunction(false, 32)));

        ThreadLocalRandom rand = ThreadLocalRandom.current();

        try (IgniteDataStreamer<Integer, CacheEntityThreeFields> streamer = ignite.dataStreamer(cacheName)) {
            for (int i = 0; i < 10_000; i++)
                streamer.addData(i, new CacheEntityThreeFields(rand.nextInt(), valueOf(rand.nextLong()), rand.nextDouble()));
        }
    }

    /**
     * Create query {@link Person} entity.
     *
     * @return Query {@link Person} entity.
     */
    static QueryEntity personEntity() {
        String orgIdField = "orgId";
        String nameField = "name";

        return new QueryEntity()
            .setKeyType(Integer.class.getName())
            .setValueType(Person.class.getName())
            .addQueryField(orgIdField, Integer.class.getName(), null)
            .addQueryField(nameField, String.class.getName(), null)
            .setIndexes(asList(new QueryIndex(nameField), new QueryIndex(orgIdField)));
    }

    /**
     * Create query {@link Person} entity.
     *
     * @return Query {@link Person} entity.
     */
    static QueryEntity organizationEntity() {
        String idField = "id";
        String nameField = "name";

        return new QueryEntity()
            .setKeyType(Integer.class.getName())
            .setValueType(Organization.class.getName())
            .addQueryField(idField, Integer.class.getName(), null)
            .addQueryField(nameField, String.class.getName(), null)
            .setIndexes(asList(new QueryIndex(nameField), new QueryIndex(idField)));
    }

    /**
     * Adds three indexes one of which is built on two fields
     * to {@code QueryEntity} provided by {@code prepareQueryEntity()}.
     *
     * @return {@code QueryEntity} with indexes.
     */
    static QueryEntity complexIndexEntity() {
        QueryEntity entity = prepareQueryEntity();

        entity.setIndexes(asList(
            new QueryIndex(ID_NAME),
            new QueryIndex(STR_NAME),
            new QueryIndex(asList(STR_NAME, DOUBLE_NAME), QueryIndexType.SORTED))
        );

        return entity;
    }

    /**
     * Adds three indexes built on single fields
     * to {@code QueryEntity} provided by {@code prepareQueryEntity()}.
     *
     * @return {@code QueryEntity} with indexes.
     */
    static QueryEntity simpleIndexEntity() {
        QueryEntity entity = prepareQueryEntity();

        entity.setIndexes(asList(
            new QueryIndex(ID_NAME),
            new QueryIndex(STR_NAME),
            new QueryIndex(DOUBLE_NAME))
        );

        return entity;
    }

    /**
     * Creates test three field entity.
     *
     * @return new {@code QueryEntity}.
     */
    private static QueryEntity prepareQueryEntity() {
        QueryEntity entity = new QueryEntity();

        entity.setKeyType(Integer.class.getName());
        entity.setValueType(CacheEntityThreeFields.class.getName());

        entity.addQueryField(ID_NAME, Integer.class.getName(), null);
        entity.addQueryField(STR_NAME, String.class.getName(), null);
        entity.addQueryField(DOUBLE_NAME, Double.class.getName(), null);

        return entity;
    }

    /**
     * Simple class Person for tests.
     */
    static class Person implements Serializable {
        /** Id organization. */
        int orgId;

        /** Name organization. */
        String name;

        /** Address of organization. */
        String orgAddr;

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

        /**
         * Set address of organization.
         *
         * @param orgAddr Address of organization.
         * @return Current instance.
         */
        public Person orgAddr(String orgAddr) {
            this.orgAddr = orgAddr;

            return this;
        }
    }

    /**
     * Simple class Organization for tests.
     */
    static class Organization implements Serializable {
        /** Id. */
        int id;

        /** Name. */
        String name;

        /** Address. */
        String addr;

        /**
         * Constructor.
         *
         * @param id Id.
         * @param name Name.
         */
        Organization(int id, String name) {
            this.id = id;
            this.name = name;
        }

        /**
         * Set address.
         *
         * @param addr Address.
         * @return Current instance.
         */
        public Organization addr(String addr) {
            this.addr = addr;

            return this;
        }
    }

    /**
     * Simple class for tests. Used for complex indexes.
     */
    static class CacheEntityThreeFields implements Serializable {
        /** */
        public static final String ID_NAME = "id";

        /** */
        public static final String STR_NAME = "strField";

        /** */
        public static final String DOUBLE_NAME = "boubleField";

        /** Id. */
        int id;

        /** String field. */
        String strField;

        /** Double field. */
        double doubleField;

        /** */
        CacheEntityThreeFields(int id, String strField, double doubleField) {
            this.id = id;
            this.strField = strField;
            this.doubleField = doubleField;
        }
    }

    /**
     * Creates several caches with different indexes. Fills them with random values.
     *
     * @param ignite Ignite instance.
     */
    static void createAndFillSeveralCaches(final Ignite ignite) {
        createAndFillCache(ignite, CACHE_NAME, GROUP_NAME);

        createAndFillThreeFieldsEntryCache(ignite, "test_" + THREE_ENTRIES_CACHE_NAME_COMMON_PART + "_complex_index",
            GROUP_NAME, asList(complexIndexEntity()));

        createAndFillCache(ignite, CACHE_NAME_SECOND, GROUP_NAME_SECOND);

        createAndFillThreeFieldsEntryCache(ignite, THREE_ENTRIES_CACHE_NAME_COMMON_PART + "_simple_indexes",
            null, asList(simpleIndexEntity()));

        createAndFillThreeFieldsEntryCache(ignite, THREE_ENTRIES_CACHE_NAME_COMMON_PART + "_no_indexes",
            null, Collections.emptyList());
    }
}
