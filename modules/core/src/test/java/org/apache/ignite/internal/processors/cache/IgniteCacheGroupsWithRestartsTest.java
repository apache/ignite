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

package org.apache.ignite.internal.processors.cache;

import com.google.common.collect.Sets;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.Lock;
import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.configuration.Factory;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryUpdatedListener;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.CacheExistsException;
import org.apache.ignite.cache.CacheInterceptor;
import org.apache.ignite.cache.CacheInterceptorAdapter;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.AffinityKeyMapper;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.platform.cache.expiry.PlatformExpiryPolicyFactory;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.lang.GridIterator;
import org.apache.ignite.internal.util.lang.GridPlainCallable;
import org.apache.ignite.internal.util.lang.gridfunc.ContainsPredicate;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.GridTestUtils.SF;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 *
 */
@SuppressWarnings({"unchecked", "ThrowableNotThrown"})
public class IgniteCacheGroupsWithRestartsTest extends GridCommonAbstractTest {
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration configuration = super.getConfiguration(gridName);

        DataStorageConfiguration cfg = new DataStorageConfiguration();

        cfg.setDefaultDataRegionConfiguration(new DataRegionConfiguration()
            .setPersistenceEnabled(true)
            .setMaxSize(256 * 1024 * 1024));

        configuration.setDataStorageConfiguration(cfg);

        return configuration;
    }

    private CacheConfiguration<Object, Object> getCacheConfiguration(int i) {
        return new CacheConfiguration<>(getCacheName(i))
            .setGroupName("group")
            .setAffinity(new RendezvousAffinityFunction(false, 64));
    }

    @NotNull private String getCacheName(int i) {
        return "cache-" + i;
    }

    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }


    @Test
    public void test() throws Exception {
        IgniteEx ex = startGrids(3);

        ex.cluster().active(true);


        for (int j = 0; j < 3; j++) {
            for (int i = 0; i < 64 * 10; i++) {
                IgniteCache<Object, Object> cache = ex.getOrCreateCache(getCacheConfiguration(j));

                byte[] val = new byte[ThreadLocalRandom.current().nextInt(8148)];

                Arrays.fill(val, (byte)i);

                cache.put(i, val);
            }
        }

        ex.destroyCache(getCacheName(0));

        assertNull(ex.cachex(getCacheName(0)));

        IgniteProcessProxy.kill(grid(2).configuration().getIgniteInstanceName());

        startGrid(2);

        assertNull(ex.cachex(getCacheName(0)));

        IgniteCache<Object, Object> cache = ex.createCache(getCacheConfiguration(0));

        awaitPartitionMapExchange();

        assertEquals(0, cache.size());

    }

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return true;
    }
}
