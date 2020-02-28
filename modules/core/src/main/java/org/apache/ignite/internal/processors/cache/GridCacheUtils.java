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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.configuration.Factory;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheWriter;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheKeyConfiguration;
import org.apache.ignite.cache.CachePartialUpdateException;
import org.apache.ignite.cache.CacheServerNotFoundException;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.store.CacheStoreSessionListener;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteClientDisconnectedCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.cluster.ClusterGroupEmptyCheckedException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.cluster.ClusterTopologyServerNotFoundException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.affinity.LocalAffinityFunction;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedLockCancelledException;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtInvalidPartitionException;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.mvcc.txlog.TxLog;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cluster.DiscoveryDataClusterState;
import org.apache.ignite.internal.processors.dr.GridDrType;
import org.apache.ignite.internal.processors.igfs.IgfsUtils;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.schema.SchemaOperationException;
import org.apache.ignite.internal.transactions.IgniteTxRollbackCheckedException;
import org.apache.ignite.internal.util.lang.GridClosureException;
import org.apache.ignite.internal.util.lang.IgniteInClosureX;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.P1;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteReducer;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.plugin.CachePluginConfiguration;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.spi.encryption.EncryptionSpi;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionRollbackException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheRebalanceMode.ASYNC;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;
import static org.apache.ignite.configuration.CacheConfiguration.DFLT_CACHE_MODE;
import static org.apache.ignite.internal.GridTopic.TOPIC_REPLICATION;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.READ;
import static org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager.SYSTEM_DATA_REGION_NAME;

/**
 * Cache utility methods.
 */
public class GridCacheUtils {
    /** Cheat cache ID for debugging and benchmarking purposes. */
    public static final int cheatCacheId;

    /** Each cache operation removes this amount of entries with expired TTL. */
    private static final int TTL_BATCH_SIZE = IgniteSystemProperties.getInteger(
        IgniteSystemProperties.IGNITE_TTL_EXPIRE_BATCH_SIZE, 5);

    /** */
    public static final int UNDEFINED_CACHE_ID = 0;

    /*
     *
     */
    static {
        String cheatCache = System.getProperty("CHEAT_CACHE");

        if (cheatCache != null) {
            cheatCacheId = cheatCache.hashCode();

            if (cheatCacheId == 0)
                throw new RuntimeException();

            System.out.println(">>> Cheat cache ID [id=" + cheatCacheId + ", name=" + cheatCache + ']');
        }
        else
            cheatCacheId = 0;
    }

    /**
     * Quickly checks if passed in cache ID is a "cheat cache ID" set by -DCHEAT_CACHE=user_cache_name
     * and resolved in static block above.
     *
     * FOR DEBUGGING AND TESTING PURPOSES!
     *
     * @param id Cache ID to check.
     * @return {@code True} if this is cheat cache ID.
     */
    @Deprecated
    public static boolean cheatCache(int id) {
        return cheatCacheId != 0 && id == cheatCacheId;
    }

    /**  Hadoop syste cache name. */
    public static final String SYS_CACHE_HADOOP_MR = "ignite-hadoop-mr-sys-cache";

    /** System cache name. */
    public static final String UTILITY_CACHE_NAME = "ignite-sys-cache";

    /** Reserved cache names */
    public static final String[] RESERVED_NAMES = new String[] {
        SYS_CACHE_HADOOP_MR,
        UTILITY_CACHE_NAME,
        MetaStorage.METASTORAGE_CACHE_NAME,
        TxLog.TX_LOG_CACHE_NAME,
    };

    /** */
    public static final String CONTINUOUS_QRY_LOG_CATEGORY = "org.apache.ignite.continuous.query";

    /** */
    public static final String CACHE_MSG_LOG_CATEGORY = "org.apache.ignite.cache.msg";

    /** */
    public static final String ATOMIC_MSG_LOG_CATEGORY = CACHE_MSG_LOG_CATEGORY + ".atomic";

    /** */
    public static final String TX_MSG_LOG_CATEGORY = CACHE_MSG_LOG_CATEGORY + ".tx";

    /** */
    public static final String TX_MSG_PREPARE_LOG_CATEGORY = TX_MSG_LOG_CATEGORY + ".prepare";

    /** */
    public static final String TX_MSG_FINISH_LOG_CATEGORY = TX_MSG_LOG_CATEGORY + ".finish";

    /** */
    public static final String TX_MSG_LOCK_LOG_CATEGORY = TX_MSG_LOG_CATEGORY + ".lock";

    /** */
    public static final String TX_MSG_RECOVERY_LOG_CATEGORY = TX_MSG_LOG_CATEGORY + ".recovery";

    /** Default mask name. */
    private static final String DEFAULT_MASK_NAME = "<default>";

    /** TTL: minimum positive value. */
    public static final long TTL_MINIMUM = 1L;

    /** TTL: eternal. */
    public static final long TTL_ETERNAL = 0L;

    /** TTL: not changed. */
    public static final long TTL_NOT_CHANGED = -1L;

    /** TTL: zero (immediate expiration). */
    public static final long TTL_ZERO = -2L;

    /** Expire time: eternal. */
    public static final long EXPIRE_TIME_ETERNAL = 0L;

    /** Expire time: must be calculated based on TTL value. */
    public static final long EXPIRE_TIME_CALCULATE = -1L;

    /** Empty predicate array. */
    private static final IgnitePredicate[] EMPTY = new IgnitePredicate[0];

    /** Default transaction config. */
    private static final TransactionConfiguration DEFAULT_TX_CFG = new TransactionConfiguration();

    /** Empty predicate array. */
    private static final IgnitePredicate[] EMPTY_FILTER = new IgnitePredicate[0];

    /** Empty predicate array. */
    private static final CacheEntryPredicate[] EMPTY_FILTER0 = new CacheEntryPredicate[0];

    /** */
    private static final CacheEntryPredicate ALWAYS_FALSE0 = new CacheEntrySerializablePredicate(
        new CacheEntryPredicateAdapter() {
            @Override public boolean apply(GridCacheEntryEx e) {
                return false;
            }
        }
    );

    /** */
    private static final CacheEntryPredicate[] ALWAYS_FALSE0_ARR = new CacheEntryPredicate[] {ALWAYS_FALSE0};

    /** Read filter. */
    public static final IgnitePredicate READ_FILTER = new P1<IgniteTxEntry>() {
        @Override public boolean apply(IgniteTxEntry e) {
            return e.op() == READ;
        }

        @Override public String toString() {
            return "READ_FILTER";
        }
    };

    /** Read filter. */
    public static final IgnitePredicate READ_FILTER_NEAR = new P1<IgniteTxEntry>() {
        @Override public boolean apply(IgniteTxEntry e) {
            return e.op() == READ && e.context().isNear();
        }

        @Override public String toString() {
            return "READ_FILTER_NEAR";
        }
    };

    /** Read filter. */
    public static final IgnitePredicate READ_FILTER_COLOCATED = new P1<IgniteTxEntry>() {
        @Override public boolean apply(IgniteTxEntry e) {
            return e.op() == READ && !e.context().isNear();
        }

        @Override public String toString() {
            return "READ_FILTER_COLOCATED";
        }
    };

    /** Write filter. */
    public static final IgnitePredicate WRITE_FILTER = new P1<IgniteTxEntry>() {
        @Override public boolean apply(IgniteTxEntry e) {
            return e.op() != READ;
        }

        @Override public String toString() {
            return "WRITE_FILTER";
        }
    };

    /** Write filter. */
    public static final IgnitePredicate WRITE_FILTER_NEAR = new P1<IgniteTxEntry>() {
        @Override public boolean apply(IgniteTxEntry e) {
            return e.op() != READ && e.context().isNear();
        }

        @Override public String toString() {
            return "WRITE_FILTER_NEAR";
        }
    };

    /** Write filter. */
    public static final IgnitePredicate WRITE_FILTER_COLOCATED = new P1<IgniteTxEntry>() {
        @Override public boolean apply(IgniteTxEntry e) {
            return e.op() != READ && !e.context().isNear();
        }

        @Override public String toString() {
            return "WRITE_FILTER_COLOCATED";
        }
    };

    /** Write filter. */
    public static final IgnitePredicate FILTER_NEAR_CACHE_ENTRY = new P1<IgniteTxEntry>() {
        @Override public boolean apply(IgniteTxEntry e) {
            return e.context().isNear();
        }

        @Override public String toString() {
            return "FILTER_NEAR_CACHE_ENTRY";
        }
    };

    /** Transaction entry to key. */
    private static final IgniteClosure tx2key = new C1<IgniteTxEntry, Object>() {
        @Override public Object apply(IgniteTxEntry e) {
            return e.key();
        }

        @Override public String toString() {
            return "Cache transaction entry to key converter.";
        }
    };

    /** Transaction entry to key. */
    private static final IgniteClosure txCol2key = new C1<Collection<IgniteTxEntry>, Collection<Object>>() {
        @SuppressWarnings( {"unchecked"})
        @Override public Collection<Object> apply(Collection<IgniteTxEntry> e) {
            return F.viewReadOnly(e, tx2key);
        }

        @Override public String toString() {
            return "Cache transaction entry collection to key collection converter.";
        }
    };

    /** Converts transaction to XID version. */
    private static final IgniteClosure tx2xidVer = new C1<IgniteInternalTx, GridCacheVersion>() {
        @Override public GridCacheVersion apply(IgniteInternalTx tx) {
            return tx.xidVersion();
        }

        @Override public String toString() {
            return "Transaction to XID version converter.";
        }
    };

    /** Converts tx entry to entry. */
    private static final IgniteClosure tx2entry = new C1<IgniteTxEntry, GridCacheEntryEx>() {
        @Override public GridCacheEntryEx apply(IgniteTxEntry e) {
            return e.cached();
        }
    };

    /** Transaction entry to key. */
    private static final IgniteClosure entry2key = new C1<GridCacheEntryEx, KeyCacheObject>() {
        @Override public KeyCacheObject apply(GridCacheEntryEx e) {
            return e.key();
        }

        @Override public String toString() {
            return "Cache extended entry to key converter.";
        }
    };

    /** Transaction entry to key. */
    private static final IgniteClosure info2key = new C1<GridCacheEntryInfo, Object>() {
        @Override public Object apply(GridCacheEntryInfo e) {
            return e.key();
        }

        @Override public String toString() {
            return "Cache extended entry to key converter.";
        }
    };

    /**
     * Ensure singleton.
     */
    protected GridCacheUtils() {
        // No-op.
    }

    /**
     * @param err If {@code true}, then throw {@link GridCacheFilterFailedException},
     *      otherwise return {@code val} passed in.
     * @return Always return {@code null}.
     * @throws GridCacheFilterFailedException If {@code err} flag is {@code true}.
     */
    @Nullable public static CacheObject failed(boolean err) throws GridCacheFilterFailedException {
        return failed(err, null);
    }

    /**
     * @param err If {@code true}, then throw {@link GridCacheFilterFailedException},
     *      otherwise return {@code val} passed in.
     * @param val Value for which evaluation happened.
     * @return Always return {@code val} passed in or throw exception.
     * @throws GridCacheFilterFailedException If {@code err} flag is {@code true}.
     */
    @Nullable public static CacheObject failed(boolean err, CacheObject val) throws GridCacheFilterFailedException {
        if (err)
            throw new GridCacheFilterFailedException(val);

        return null;
    }

    /**
     * Create filter array.
     *
     * @param filter Filter.
     * @return Filter array.
     */
    public static CacheEntryPredicate[] filterArray(@Nullable CacheEntryPredicate filter) {
        return filter != null ? new CacheEntryPredicate[] { filter } : CU.empty0();
    }

    /**
     * Entry predicate factory mostly used for deserialization.
     *
     * @param <K> Key type.
     * @param <V> Value type.
     * @return Factory instance.
     */
    public static <K, V> IgniteClosure<Integer, IgnitePredicate<Cache.Entry<K, V>>[]> factory() {
        return new IgniteClosure<Integer, IgnitePredicate<Cache.Entry<K, V>>[]>() {
            @Override public IgnitePredicate<Cache.Entry<K, V>>[] apply(Integer len) {
                return (IgnitePredicate<Cache.Entry<K, V>>[])(len == 0 ? EMPTY : new IgnitePredicate[len]);
            }
        };
    }

    /**
     * Checks that cache store is present.
     *
     * @param ctx Registry.
     * @throws IgniteCheckedException If cache store is not present.
     */
    public static void checkStore(GridCacheContext<?, ?> ctx) throws IgniteCheckedException {
        if (!ctx.store().configured())
            throw new IgniteCheckedException("Failed to find cache store for method 'reload(..)' " +
                "(is GridCacheStore configured?)");
    }

    /**
     * Gets DHT affinity nodes.
     *
     * @param ctx Cache context.
     * @param topVer Topology version.
     * @return Cache affinity nodes for given topology version.
     */
    public static Collection<ClusterNode> affinityNodes(GridCacheContext ctx, AffinityTopologyVersion topVer) {
        return ctx.discovery().cacheGroupAffinityNodes(ctx.groupId(), topVer);
    }

    /**
     * Checks if near cache is enabled for cache context.
     *
     * @param ctx Cache context to check.
     * @return {@code True} if near cache is enabled, {@code false} otherwise.
     */
    public static boolean isNearEnabled(GridCacheContext ctx) {
        return isNearEnabled(ctx.config());
    }

    /**
     * Checks if near cache is enabled for cache configuration.
     *
     * @param cfg Cache configuration to check.
     * @return {@code True} if near cache is enabled, {@code false} otherwise.
     */
    @SuppressWarnings("SimplifiableIfStatement")
    public static boolean isNearEnabled(CacheConfiguration cfg) {
        if (cfg.getCacheMode() == LOCAL)
            return false;

        return cfg.getNearConfiguration() != null;
    }

    /**
     * @param nodes Nodes.
     * @return Oldest node for the given topology version.
     */
    @Nullable public static ClusterNode oldest(Collection<ClusterNode> nodes) {
        ClusterNode oldest = null;

        for (ClusterNode n : nodes) {
            if (oldest == null || n.order() < oldest.order())
                oldest = n;
        }

        return oldest;
    }

    /**
     * @return Empty filter.
     */
    public static <K, V> IgnitePredicate<Cache.Entry<K, V>>[] empty() {
        return (IgnitePredicate<Cache.Entry<K, V>>[])EMPTY_FILTER;
    }

    /**
     * @return Empty filter.
     */
    public static CacheEntryPredicate[] empty0() {
        return EMPTY_FILTER0;
    }

    /**
     * @return Always false filter.
     */
    public static CacheEntryPredicate[] alwaysFalse0Arr() {
        return ALWAYS_FALSE0_ARR;
    }

    /**
     * @return Closure which converts transaction entry xid to XID version.
     */
    public static IgniteClosure<IgniteInternalTx, GridCacheVersion> tx2xidVersion() {
        return (IgniteClosure<IgniteInternalTx, GridCacheVersion>)tx2xidVer;
    }

    /**
     * @return Closure that converts entry to key.
     */
    @SuppressWarnings({"unchecked"})
    public static IgniteClosure<GridCacheEntryEx, KeyCacheObject> entry2Key() {
        return entry2key;
    }

    /**
     * @return Closure that converts entry info to key.
     */
    public static <K, V> IgniteClosure<GridCacheEntryInfo, K> info2Key() {
        return (IgniteClosure<GridCacheEntryInfo, K>)info2key;
    }

    /**
     * @return Filter for transaction reads.
     */
    @SuppressWarnings({"unchecked"})
    public static IgnitePredicate<IgniteTxEntry> reads() {
        return READ_FILTER;
    }

    /**
     * @return Filter for transaction writes.
     */
    @SuppressWarnings({"unchecked"})
    public static IgnitePredicate<IgniteTxEntry> writes() {
        return WRITE_FILTER;
    }

    /**
     * @return Boolean reducer.
     */
    public static IgniteReducer<Boolean, Boolean> boolReducer() {
        return new IgniteReducer<Boolean, Boolean>() {
            private final AtomicBoolean bool = new AtomicBoolean(true);

            @Override public boolean collect(Boolean b) {
                bool.compareAndSet(true, b);

                // Stop collecting on first failure.
                return bool.get();
            }

            @Override public Boolean reduce() {
                return bool.get();
            }

            @Override public String toString() {
                return "Bool reducer: " + bool;
            }
        };
    }

    /**
     * @return Long reducer.
     */
    public static IgniteReducer<Long, Long> longReducer() {
        return new IgniteReducer<Long, Long>() {
            private final LongAdder res = new LongAdder();

            @Override public boolean collect(Long l) {
                if(l != null)
                    res.add(l);

                return true;
            }

            @Override public Long reduce() {
                return res.sum();
            }

            @Override public String toString() {
                return "Long reducer: " + res;
            }
        };
    }

    /**
     * Gets reducer that aggregates maps into one.
     *
     * @param size Predicted size of the resulting map to avoid resizings.
     * @param <K> Key type.
     * @param <V> Value type.
     * @return Reducer.
     */
    public static <K, V> IgniteReducer<Map<K, V>, Map<K, V>> mapsReducer(final int size) {
        return new IgniteReducer<Map<K, V>, Map<K, V>>() {
            private final Map<K, V> ret = new ConcurrentHashMap<>(size);

            @Override public boolean collect(Map<K, V> map) {
                if (map != null)
                    ret.putAll(map);

                return true;
            }

            @Override public Map<K, V> reduce() {
                return ret;
            }

            /** {@inheritDoc} */
            @Override public String toString() {
                return "Map reducer: " + ret;
            }
        };
    }

    /**
     * Gets reducer that aggregates collections.
     *
     * @param <T> Collection element type.
     * @return Reducer.
     */
    public static <T> IgniteReducer<Collection<T>, Collection<T>> collectionsReducer(final int size) {
        return new IgniteReducer<Collection<T>, Collection<T>>() {
            private List<T> ret;

            @Override public synchronized boolean collect(Collection<T> c) {
                if (c == null)
                    return true;

                if (ret == null)
                    ret = new ArrayList<>(size);

                ret.addAll(c);

                return true;
            }

            @Override public synchronized Collection<T> reduce() {
                return ret == null ? Collections.<T>emptyList() : ret;
            }

            /** {@inheritDoc} */
            @Override public synchronized String toString() {
                return "Collection reducer: " + ret;
            }
        };
    }

    /**
     * Gets reducer that aggregates items into collection.
     *
     * @param <T> Items type.
     * @return Reducer.
     */
    public static <T> IgniteReducer<T, Collection<T>> objectsReducer() {
        return new IgniteReducer<T, Collection<T>>() {
            private final Collection<T> ret = new ConcurrentLinkedQueue<>();

            @Override public boolean collect(T item) {
                if (item != null)
                    ret.add(item);

                return true;
            }

            @Override public Collection<T> reduce() {
                return ret;
            }
        };
    }

    /**
     *
     * @param nodes Set of nodes.
     * @return Primary node.
     */
    public static ClusterNode primary(Iterable<? extends ClusterNode> nodes) {
        ClusterNode n = F.first(nodes);

        assert n != null;

        return n;
    }

    /**
     * @param nodes Nodes.
     * @return Backup nodes.
     */
    public static Collection<ClusterNode> backups(Collection<ClusterNode> nodes) {
        if (nodes == null || nodes.size() <= 1)
            return Collections.emptyList();

        return F.view(nodes, F.notEqualTo(F.first(nodes)));
    }

    /**
     * @param log Logger.
     * @param excl Excludes.
     * @return Future listener that logs errors.
     */
    public static IgniteInClosure<IgniteInternalFuture<?>> errorLogger(final IgniteLogger log,
        final Class<? extends Exception>... excl) {
        return new CI1<IgniteInternalFuture<?>>() {
            @Override public void apply(IgniteInternalFuture<?> f) {
                try {
                    f.get();
                }
                catch (IgniteCheckedException e) {
                    if (!F.isEmpty(excl))
                        for (Class cls : excl)
                            if (e.hasCause(cls))
                                return;

                    U.error(log, "Future execution resulted in error: " + f, e);
                }
            }

            @Override public String toString() {
                return "Error logger [excludes=" + Arrays.toString(excl) + ']';
            }
        };
    }

    /**
     * @param t Exception to check.
     * @return {@code true} if caused by lock timeout or cancellation.
     */
    public static boolean isLockTimeoutOrCancelled(Throwable t) {
        if (t == null)
            return false;

        while (t instanceof IgniteCheckedException || t instanceof IgniteException)
            t = t.getCause();

        return t instanceof GridCacheLockTimeoutException || t instanceof GridDistributedLockCancelledException;
    }

    /**
     * @param ctx Cache context.
     * @param obj Object to marshal.
     * @return Buffer that contains obtained byte array.
     * @throws IgniteCheckedException If marshalling failed.
     */
    public static byte[] marshal(GridCacheContext ctx, Object obj)
        throws IgniteCheckedException {
        assert ctx != null;

        return marshal(ctx.shared(), ctx.deploymentEnabled(), obj);
    }

    /**
     * @param ctx Cache context.
     * @param depEnabled deployment enabled flag.
     * @param obj Object to marshal.
     * @return Buffer that contains obtained byte array.
     * @throws IgniteCheckedException If marshalling failed.
     */
    public static byte[] marshal(GridCacheSharedContext ctx, boolean depEnabled, Object obj)
        throws IgniteCheckedException {
        assert ctx != null;

        if (depEnabled) {
            if (obj != null) {
                if (obj instanceof Iterable)
                    ctx.deploy().registerClasses((Iterable<?>)obj);
                else if (obj.getClass().isArray()) {
                    if (!U.isPrimitiveArray(obj))
                        ctx.deploy().registerClasses((Object[])obj);
                }
                else
                    ctx.deploy().registerClass(obj);
            }
        }

        return U.marshal(ctx, obj);
    }

    /**
     * @param val Value.
     * @param skip Skip value flag.
     * @return Value.
     */
    public static Object skipValue(Object val, boolean skip) {
        if (skip)
            return val != null ? true : null;
        else
            return val;
    }

    /**
     * @param ctx Context.
     * @param prj Projection.
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @return New transaction.
     */
    public static GridNearTxLocal txStartInternal(GridCacheContext ctx, IgniteInternalCache prj,
        TransactionConcurrency concurrency, TransactionIsolation isolation) {
        assert ctx != null;
        assert prj != null;

        ctx.tm().resetContext();

        return prj.txStartEx(concurrency, isolation);
    }

    /**
     * Alias for {@link #txString(IgniteInternalTx)}.
     */
    public static String txDump(@Nullable IgniteInternalTx tx) {
        return txString(tx);
    }

    /**
     * @param tx Transaction.
     * @return String view of all safe-to-print transaction properties.
     */
    public static String txString(@Nullable IgniteInternalTx tx) {
        if (tx == null)
            return "null";

        return tx.getClass().getSimpleName() + "[xid=" + tx.xid() +
            ", xidVersion=" + tx.xidVersion() +
            ", nearXidVersion=" + tx.nearXidVersion() +
            ", concurrency=" + tx.concurrency() +
            ", isolation=" + tx.isolation() +
            ", state=" + tx.state() +
            ", invalidate=" + tx.isInvalidate() +
            ", rollbackOnly=" + tx.isRollbackOnly() +
            ", nodeId=" + tx.nodeId() +
            ", timeout=" + tx.timeout() +
            ", startTime=" + tx.startTime() +
            ", duration=" + (U.currentTimeMillis() - tx.startTime()) +
            (tx instanceof GridNearTxLocal ? ", label=" + tx.label() : "") +
            ']';
    }

    /**
     * @param ctx Cache context.
     */
    public static void unwindEvicts(GridCacheContext ctx) {
        assert ctx != null;

        ctx.ttl().expire(TTL_BATCH_SIZE);
    }

    /**
     * @param ctx Shared cache context.
     */
    public static <K, V> void unwindEvicts(GridCacheSharedContext<K, V> ctx) {
        assert ctx != null;

        for (GridCacheContext<K, V> cacheCtx : ctx.cacheContexts())
            unwindEvicts(cacheCtx);
    }

    /**
     * @param asc {@code True} for ascending.
     * @return Descending order comparator.
     */
    public static Comparator<ClusterNode> nodeComparator(final boolean asc) {
        return new Comparator<ClusterNode>() {
            @Override public int compare(ClusterNode n1, ClusterNode n2) {
                long o1 = n1.order();
                long o2 = n2.order();

                return asc ? o1 < o2 ? -1 : o1 == o2 ? 0 : 1 : o1 < o2 ? 1 : o1 == o2 ? 0 : -1;
            }

            @Override public String toString() {
                return "Node comparator [asc=" + asc + ']';
            }
        };
    }

    /**
     * Mask cache name in case it is null.
     *
     * @param cacheName Cache name.
     * @return The same cache name or {@code <default>} in case the name is {@code null}.
     */
    public static String mask(String cacheName) {
        return cacheName != null ? cacheName : DEFAULT_MASK_NAME;
    }

    /**
     * Unmask cache name.
     *
     * @param cacheName Cache name.
     * @return Unmasked cache name, i.e. in case provided parameter was {@code <default>} then {@code null}
     *     will be returned.
     */
    @Nullable public static String unmask(String cacheName) {
        return DEFAULT_MASK_NAME.equals(cacheName) ? null : cacheName;
    }

    /**
     * Get topic to which replication requests are sent.
     *
     * @return Topic to which replication requests are sent.
     */
    public static String replicationTopicSend() {
        return TOPIC_REPLICATION.toString();
    }

    /**
     * Get topic to which replication responses are sent.
     *
     * @param cacheName Cache name.
     * @return Topic to which replication responses are sent.
     */
    public static String replicationTopicReceive(String cacheName) {
        return TOPIC_REPLICATION + "-" + mask(cacheName);
    }

    /**
     * Checks that local and remove configurations have the same value of given attribute.
     *
     * @param log Logger used to log warning message (used only if fail flag is not set).
     * @param locCfg Local configuration.
     * @param rmtCfg Remote configuration.
     * @param rmtNodeId Remote node.
     * @param attr Attribute name.
     * @param fail If true throws IgniteCheckedException in case of attribute values mismatch, otherwise logs warning.
     * @throws IgniteCheckedException If attribute values are different and fail flag is true.
     */
    public static void checkAttributeMismatch(IgniteLogger log, CacheConfiguration locCfg,
        CacheConfiguration rmtCfg, UUID rmtNodeId, T2<String, String> attr, boolean fail) throws IgniteCheckedException {
        assert rmtNodeId != null;
        assert attr != null;
        assert attr.get1() != null;
        assert attr.get2() != null;

        Object locVal = U.property(locCfg, attr.get1());

        Object rmtVal = U.property(rmtCfg, attr.get1());

        checkAttributeMismatch(log, rmtCfg.getName(), rmtNodeId, attr.get1(), attr.get2(), locVal, rmtVal, fail);
    }

    /**
     * Checks that cache configuration attribute has the same value in local and remote cache configurations.
     *
     * @param log Logger used to log warning message (used only if fail flag is not set).
     * @param cfgName Remote cache name.
     * @param rmtNodeId Remote node.
     * @param attrName Short attribute name for error message.
     * @param attrMsg Full attribute name for error message.
     * @param locVal Local value.
     * @param rmtVal Remote value.
     * @param fail If true throws IgniteCheckedException in case of attribute values mismatch, otherwise logs warning.
     * @throws IgniteCheckedException If attribute values are different and fail flag is true.
     */
    public static void checkAttributeMismatch(IgniteLogger log, String cfgName, UUID rmtNodeId, String attrName,
        String attrMsg, @Nullable Object locVal, @Nullable Object rmtVal, boolean fail) throws IgniteCheckedException {
        assert rmtNodeId != null;
        assert attrName != null;
        assert attrMsg != null;

        if (!F.eq(locVal, rmtVal))
            throwIgniteCheckedException(log, fail,
                attrMsg + " mismatch [" +
                    "cacheName=" + cfgName + ", " +
                    "local" + capitalize(attrName) + "=" + locVal + ", " +
                    "remote" + capitalize(attrName) + "=" + rmtVal + ", " +
                    "rmtNodeId=" + rmtNodeId + ']');
    }

    /**
     * @param cfg1 Existing configuration.
     * @param cfg2 Cache configuration to start.
     * @param attrName Short attribute name for error message.
     * @param attrMsg Full attribute name for error message.
     * @param val1 Attribute value in existing configuration.
     * @param val2 Attribute value in starting configuration.
     * @param fail If true throws IgniteCheckedException in case of attribute values mismatch, otherwise logs warning.
     * @throws IgniteCheckedException If validation failed.
     */
    public static void validateCacheGroupsAttributesMismatch(IgniteLogger log,
        CacheConfiguration cfg1,
        CacheConfiguration cfg2,
        String attrName,
        String attrMsg,
        Object val1,
        Object val2,
        boolean fail) throws IgniteCheckedException {
        if (F.eq(val1, val2))
            return;

        if (fail) {
            throw new IgniteCheckedException(attrMsg + " mismatch for caches related to the same group " +
                "[groupName=" + cfg1.getGroupName() +
                ", existingCache=" + cfg1.getName() +
                ", existing" + capitalize(attrName) + "=" + val1 +
                ", startingCache=" + cfg2.getName() +
                ", starting" + capitalize(attrName) + "=" + val2 + ']');
        }
        else {
            U.warn(log, attrMsg + " mismatch for caches related to the same group " +
                "[groupName=" + cfg1.getGroupName() +
                ", existingCache=" + cfg1.getName() +
                ", existing" + capitalize(attrName) + "=" + val1 +
                ", startingCache=" + cfg2.getName() +
                ", starting" + capitalize(attrName) + "=" + val2 + ']');
        }
    }

    /**
     * @param str String.
     * @return String with first symbol in upper case.
     */
    private static String capitalize(String str) {
        return Character.toUpperCase(str.charAt(0)) + str.substring(1);
    }

    /**
     * Validates that cache key object has overridden equals and hashCode methods.
     * Will also check that a BinaryObject has a hash code set.
     *
     * @param key Key.
     * @throws IllegalArgumentException If equals or hashCode is not implemented.
     */
    public static void validateCacheKey(@Nullable Object key) {
        if (key == null)
            return;

        if (!U.overridesEqualsAndHashCode(key))
            throw new IllegalArgumentException("Cache key must override hashCode() and equals() methods: " +
                key.getClass().getName());
    }

    /**
     * @param cacheName Cache name.
     * @return {@code True} if this is Hadoop system cache.
     */
    public static boolean isHadoopSystemCache(String cacheName) {
        return F.eq(cacheName, SYS_CACHE_HADOOP_MR);
    }

    /**
     * Create system cache used by Hadoop component.
     *
     * @return Hadoop cache configuration.
     */
    public static CacheConfiguration hadoopSystemCache() {
        CacheConfiguration cache = new CacheConfiguration();

        cache.setName(CU.SYS_CACHE_HADOOP_MR);
        cache.setCacheMode(REPLICATED);
        cache.setAtomicityMode(TRANSACTIONAL);
        cache.setWriteSynchronizationMode(FULL_SYNC);

        cache.setEvictionPolicyFactory(null);
        cache.setEvictionPolicy(null);
        cache.setCacheStoreFactory(null);
        cache.setNodeFilter(CacheConfiguration.ALL_NODES);
        cache.setEagerTtl(true);
        cache.setRebalanceMode(SYNC);

        return cache;
    }

    /**
     * @param cacheName Cache name.
     * @return {@code True} if this is utility system cache.
     */
    public static boolean isUtilityCache(String cacheName) {
        return UTILITY_CACHE_NAME.equals(cacheName);
    }

    /**
     * @param cacheName Cache name.
     * @return {@code True} if system cache.
     */
    public static boolean isSystemCache(String cacheName) {
        return isUtilityCache(cacheName) || isHadoopSystemCache(cacheName);
    }

    /**
     * @param cacheName Cache name.
     * @return Cache ID.
     */
    public static int cacheId(String cacheName) {
        if (cacheName != null) {
            int hash = cacheName.hashCode();

            if (hash == 0)
                hash = 1;

            return hash;
        }
        else
            return 1;
    }

    /**
     * @param cacheName Cache name.
     * @param grpName Group name.
     * @return Group ID.
     */
    public static int cacheGroupId(String cacheName, @Nullable String grpName) {
        assert cacheName != null;

        return grpName != null ? CU.cacheId(grpName) : CU.cacheId(cacheName);
    }

    /**
     * @param cfg Grid configuration.
     * @param cacheName Cache name.
     * @return {@code True} in this is IGFS data or meta cache.
     */
    public static boolean isIgfsCache(IgniteConfiguration cfg, @Nullable String cacheName) {
        return IgfsUtils.isIgfsCache(cfg, cacheName);
    }

    /**
     * Convert TTL to expire time.
     *
     * @param ttl TTL.
     * @return Expire time.
     */
    public static long toExpireTime(long ttl) {
        assert ttl != CU.TTL_ZERO && ttl != CU.TTL_NOT_CHANGED && ttl >= 0 : "Invalid TTL: " + ttl;

        long expireTime = ttl == CU.TTL_ETERNAL ? CU.EXPIRE_TIME_ETERNAL : U.currentTimeMillis() + ttl;

        // Account for overflow.
        if (expireTime < 0)
            expireTime = CU.EXPIRE_TIME_ETERNAL;

        return expireTime;
    }

    /**
     * Execute closure inside cache transaction.
     *
     * @param cache Cache.
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @param clo Closure.
     * @throws IgniteCheckedException If failed.
     */
    public static <K, V> void inTx(IgniteInternalCache<K, V> cache, TransactionConcurrency concurrency,
        TransactionIsolation isolation, IgniteInClosureX<IgniteInternalCache<K ,V>> clo) throws IgniteCheckedException {

        try (GridNearTxLocal tx = cache.txStartEx(concurrency, isolation)) {
            clo.applyx(cache);

            tx.commit();
        }
    }

    /**
     * Execute closure inside cache transaction.
     *
     * @param cache Cache.
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @param clo Closure.
     * @throws IgniteCheckedException If failed.
     */
    public static <K, V> void inTx(Ignite ignite, IgniteCache<K, V> cache, TransactionConcurrency concurrency,
        TransactionIsolation isolation, IgniteInClosureX<IgniteCache<K ,V>> clo) throws IgniteCheckedException {

        try (Transaction tx = ignite.transactions().txStart(concurrency, isolation)) {
            clo.applyx(cache);

            tx.commit();
        }
    }

    /**
     * Gets subject ID by transaction.
     *
     * @param tx Transaction.
     * @return Subject ID.
     */
    public static <K, V> UUID subjectId(IgniteInternalTx tx, GridCacheSharedContext<K, V> ctx) {
        if (tx == null)
            return ctx.localNodeId();

        UUID subjId = tx.subjectId();

        return subjId != null ? subjId : tx.originatingNodeId();
    }

    /**
     * Invalidate entry in cache.
     *
     * @param cache Cache.
     * @param key Key.
     */
    public static <K, V> void invalidate(IgniteCache<K, V> cache, K key) {
        cache.localClear(key);
    }

    /**
     * @param duration Duration.
     * @return TTL.
     */
    public static long toTtl(Duration duration) {
        if (duration == null)
            return TTL_NOT_CHANGED;

        if (duration.getDurationAmount() == 0) {
            if (duration.isEternal())
                return TTL_ETERNAL;

            assert duration.isZero();

            return TTL_ZERO;
        }

        assert duration.getTimeUnit() != null : duration;

        return duration.getTimeUnit().toMillis(duration.getDurationAmount());
    }

    /**
     * Get TTL for load operation.
     *
     * @param plc Expiry policy.
     * @return TTL for load operation or {@link #TTL_ZERO} in case of immediate expiration.
     */
    public static long ttlForLoad(ExpiryPolicy plc) {
        if (plc != null) {
            long ttl = toTtl(plc.getExpiryForCreation());

            if (ttl == TTL_NOT_CHANGED)
                ttl = TTL_ETERNAL;

            return ttl;
        }
        else
            return TTL_ETERNAL;
    }

    /**
     * @return Expire time denoting a point in the past.
     */
    public static long expireTimeInPast() {
        return U.currentTimeMillis() - 1L;
    }

    /**
     * @param e Ignite checked exception.
     * @return CacheException runtime exception, never null.
     */
    public static @NotNull RuntimeException convertToCacheException(IgniteCheckedException e) {
        IgniteClientDisconnectedCheckedException disconnectedErr =
            e.getCause(IgniteClientDisconnectedCheckedException.class);

        if (disconnectedErr != null) {
            assert disconnectedErr.reconnectFuture() != null : disconnectedErr;

            e = disconnectedErr;
        }

        if (e.hasCause(CacheWriterException.class))
            return new CacheWriterException(U.convertExceptionNoWrap(e));

        if (e instanceof CachePartialUpdateCheckedException)
            return new CachePartialUpdateException((CachePartialUpdateCheckedException)e);
        else if (e.hasCause(ClusterTopologyServerNotFoundException.class))
            return new CacheServerNotFoundException(e.getMessage(), e);
        else if (e instanceof SchemaOperationException)
            return new CacheException(e.getMessage(), e);

        CacheException ce = X.cause(e, CacheException.class);
        if (ce != null)
            return ce;

        if (e.getCause() instanceof NullPointerException)
            return (NullPointerException)e.getCause();

        if (e.getCause() instanceof SecurityException)
            return (SecurityException)e.getCause();

        C1<IgniteCheckedException, IgniteException> converter = U.getExceptionConverter(e.getClass());

        return converter != null ? new CacheException(converter.apply(e)) : new CacheException(e);
    }

    /**
     * @param cacheObj Cache object.
     * @param ctx Cache context.
     * @param cpy Copy flag.
     * @return Cache object value.
     */
    @Nullable public static <T> T value(@Nullable CacheObject cacheObj, GridCacheContext ctx, boolean cpy) {
        return cacheObj != null ? cacheObj.<T>value(ctx.cacheObjectContext(), cpy) : null;
    }

    /**
     * @param cfg Cache configuration.
     * @param cl Type of cache plugin configuration.
     * @return Cache plugin configuration by type from cache configuration or <code>null</code>.
     */
    public static <C extends CachePluginConfiguration> C cachePluginConfiguration(
        CacheConfiguration cfg, Class<C> cl) {
        if (cfg.getPluginConfigurations() != null) {
            for (CachePluginConfiguration pluginCfg : cfg.getPluginConfigurations()) {
                if (pluginCfg.getClass() == cl)
                    return (C)pluginCfg;
            }
        }

        return null;
    }

    /**
     * @param cfg Config.
     * @param cls Class.
     * @return Not <code>null</code> list.
     */
    public static <T extends CachePluginConfiguration> List<T> cachePluginConfigurations(IgniteConfiguration cfg,
        Class<T> cls) {
        List<T> res = new ArrayList<>();

        if (cfg.getCacheConfiguration() != null) {
            for (CacheConfiguration ccfg : cfg.getCacheConfiguration()) {
                for (CachePluginConfiguration pluginCcfg : ccfg.getPluginConfigurations()) {
                    if (cls == pluginCcfg.getClass())
                        res.add((T)pluginCcfg);
                }
            }
        }

        return res;
    }

    /**
     * @param node Node.
     * @param filter Node filter.
     * @return {@code True} if node is not client node and pass given filter.
     */
    public static boolean affinityNode(ClusterNode node, IgnitePredicate<ClusterNode> filter) {
        return !node.isDaemon() && !node.isClient() && filter.apply(node);
    }

    /**
     * @param node Node.
     * @param discoveryDataClusterState Discovery data cluster state.
     * @return {@code True} if node is included in BaselineTopology.
     */
    public static boolean baselineNode(ClusterNode node, DiscoveryDataClusterState discoveryDataClusterState) {
        return discoveryDataClusterState.baselineTopology().consistentIds().contains(node.consistentId());
    }

    /**
     * Creates and starts store session listeners.
     *
     * @param ctx Kernal context.
     * @param factories Factories.
     * @return Listeners.
     * @throws IgniteCheckedException In case of error.
     */
    public static Collection<CacheStoreSessionListener> startStoreSessionListeners(GridKernalContext ctx,
        Factory<CacheStoreSessionListener>[] factories) throws IgniteCheckedException {
        if (factories == null)
            return null;

        Collection<CacheStoreSessionListener> lsnrs = new ArrayList<>(factories.length);

        for (Factory<CacheStoreSessionListener> factory : factories) {
            CacheStoreSessionListener lsnr = factory.create();

            if (lsnr != null) {
                ctx.resource().injectGeneric(lsnr);

                if (lsnr instanceof LifecycleAware)
                    ((LifecycleAware)lsnr).start();

                lsnrs.add(lsnr);
            }
        }

        return lsnrs;
    }

    /**
     * @param partsMap Cache ID to partition IDs collection map.
     * @return Cache ID to partition ID array map.
     */
    public static Map<Integer, int[]> convertInvalidPartitions(Map<Integer, Set<Integer>> partsMap) {
        Map<Integer, int[]> res = new HashMap<>(partsMap.size());

        for (Map.Entry<Integer, Set<Integer>> entry : partsMap.entrySet()) {
            Set<Integer> parts = entry.getValue();

            int[] partsArr = new int[parts.size()];

            int idx = 0;

            for (Integer part : parts)
                partsArr[idx++] = part;

            res.put(entry.getKey(), partsArr);
        }

        return res;
    }

    /**
     * Stops store session listeners.
     *
     * @param ctx Kernal context.
     * @param sesLsnrs Session listeners.
     * @throws IgniteCheckedException In case of error.
     */
    public static void stopStoreSessionListeners(GridKernalContext ctx, Collection<CacheStoreSessionListener> sesLsnrs)
        throws IgniteCheckedException {
        if (sesLsnrs == null)
            return;

        for (CacheStoreSessionListener lsnr : sesLsnrs) {
            if (lsnr instanceof LifecycleAware)
                ((LifecycleAware)lsnr).stop();

            ctx.resource().cleanupGeneric(lsnr);
        }
    }

    /**
     * @param c Closure to retry.
     * @throws IgniteCheckedException If failed.
     * @return Closure result.
     */
    public static <S> S retryTopologySafe(final Callable<S> c) throws IgniteCheckedException {
        IgniteCheckedException err = null;

        for (int i = 0; i < GridCacheAdapter.MAX_RETRIES; i++) {
            try {
                return c.call();
            }
            catch (ClusterGroupEmptyCheckedException | ClusterTopologyServerNotFoundException e) {
                throw e;
            }
            catch (TransactionRollbackException e) {
                if (i + 1 == GridCacheAdapter.MAX_RETRIES)
                    throw e;

                U.sleep(1);
            }
            catch (IgniteCheckedException e) {
                if (i + 1 == GridCacheAdapter.MAX_RETRIES)
                    throw e;

                if (X.hasCause(e, ClusterTopologyCheckedException.class)) {
                    ClusterTopologyCheckedException topErr = e.getCause(ClusterTopologyCheckedException.class);

                    if (topErr instanceof ClusterGroupEmptyCheckedException || topErr instanceof
                        ClusterTopologyServerNotFoundException)
                        throw e;

                    // IGNITE-1948: remove this check when the issue is fixed
                    if (topErr.retryReadyFuture() != null)
                        topErr.retryReadyFuture().get();
                    else
                        U.sleep(1);
                }
                else if (X.hasCause(e, IgniteTxRollbackCheckedException.class,
                    CachePartialUpdateCheckedException.class))
                    U.sleep(1);
                else
                    throw e;
            }
            catch (RuntimeException e) {
                throw e;
            }
            catch (Exception e) {
                throw new IgniteCheckedException(e);
            }
        }

        // Should never happen.
        throw err;
    }

    /**
     * Builds neighborhood map for all nodes in snapshot.
     *
     * @param topSnapshot Topology snapshot.
     * @return Neighbors map.
     */
    public static Map<UUID, Collection<ClusterNode>> neighbors(Collection<ClusterNode> topSnapshot) {
        Map<String, Collection<ClusterNode>> macMap = new HashMap<>(topSnapshot.size(), 1.0f);

        // Group by mac addresses.
        for (ClusterNode node : topSnapshot) {
            String macs = node.attribute(IgniteNodeAttributes.ATTR_MACS);

            Collection<ClusterNode> nodes = macMap.get(macs);

            if (nodes == null)
                macMap.put(macs, nodes = new HashSet<>());

            nodes.add(node);
        }

        Map<UUID, Collection<ClusterNode>> neighbors = new HashMap<>(topSnapshot.size(), 1.0f);

        for (Collection<ClusterNode> group : macMap.values())
            for (ClusterNode node : group)
                neighbors.put(node.id(), group);

        return neighbors;
    }

    /**
     * Returns neighbors for all {@code nodes}.
     *
     * @param neighborhood Neighborhood cache.
     * @param nodes Nodes.
     * @return All neighbors for given nodes.
     */
    public static Collection<ClusterNode> neighborsForNodes(Map<UUID, Collection<ClusterNode>> neighborhood,
        Iterable<ClusterNode> nodes) {
        Collection<ClusterNode> res = new HashSet<>();

        for (ClusterNode node : nodes) {
            if (!res.contains(node))
                res.addAll(neighborhood.get(node.id()));
        }

        return res;
    }

    /**
     * @return default TX configuration if system cache is used or current grid TX config otherwise.
     */
    public static TransactionConfiguration transactionConfiguration(@Nullable final GridCacheContext sysCacheCtx,
        final IgniteConfiguration cfg) {
        return sysCacheCtx != null && sysCacheCtx.systemTx()
            ? DEFAULT_TX_CFG
            : cfg.getTransactionConfiguration();
    }

    /**
     * @param name Cache name.
     * @throws IllegalArgumentException In case the name is not valid.
     */
    public static void validateCacheName(String name) throws IllegalArgumentException {
        A.ensure(name != null && !name.isEmpty(), "Cache name must not be null or empty.");
    }

    /**
     * @param name Cache name.
     * @throws IllegalArgumentException In case the name is not valid.
     */
    public static void validateNewCacheName(String name) throws IllegalArgumentException {
        validateCacheName(name);

        A.ensure(!isReservedCacheName(name), "Cache name cannot be \"" + name +
            "\" because it is reserved for internal purposes.");
    }

    /**
     * @param cacheNames Cache names to validate.
     * @throws IllegalArgumentException In case the name is not valid.
     */
    public static void validateCacheNames(Collection<String> cacheNames) throws IllegalArgumentException {
        for (String name : cacheNames)
            validateCacheName(name);
    }

    /**
     * @param ccfgs Configurations to validate.
     * @throws IllegalArgumentException In case the name is not valid.
     */
    public static void validateConfigurationCacheNames(Collection<CacheConfiguration> ccfgs)
        throws IllegalArgumentException {
        for (CacheConfiguration ccfg : ccfgs)
            validateNewCacheName(ccfg.getName());
    }

    /**
     * @param name Cache name.
     * @return {@code True} if it is a reserved cache name.
     */
    public static boolean isReservedCacheName(String name) {
        for (String reserved : RESERVED_NAMES) {
            if (reserved.equals(name))
                return true;
        }

        return false;
    }

    /**
     * Validate affinity key configurations.
     * All fields are initialized and not empty (typeName and affKeyFieldName is defined).
     * Definition for the type does not repeat.
     *
     * @param groupName Cache group name.
     * @param cacheName Cache name.
     * @param cacheKeyCfgs keyConfiguration to validate.
     * @param log Logger used to log warning message (used only if fail flag is not set).
     * @param fail If true throws IgniteCheckedException in case of attribute values mismatch, otherwise logs warning.
     * @return Affinity key maps (typeName -> fieldName)
     * @throws IgniteCheckedException In case the affinity key configurations is not valid.
     */
    public static Map<String, String> validateKeyConfigiration(
        String groupName, String cacheName, CacheKeyConfiguration[] cacheKeyCfgs, IgniteLogger log,
        boolean fail
    ) throws IgniteCheckedException {
        Map<String, String> keyConfigurations = new HashMap<>();

        if (cacheKeyCfgs != null) {
            for (CacheKeyConfiguration cacheKeyCfg : cacheKeyCfgs) {
                String typeName = cacheKeyCfg.getTypeName();

                A.notNullOrEmpty(typeName, "typeName");

                String fieldName = cacheKeyCfg.getAffinityKeyFieldName();

                A.notNullOrEmpty(fieldName, "affKeyFieldName");

                String oldFieldName = keyConfigurations.put(typeName, fieldName);

                if (oldFieldName != null && !oldFieldName.equals(fieldName)) {
                    final String msg = "Cache key configuration contains conflicting definitions: [" +
                        (groupName != null ? "cacheGroup=" + groupName + ", " : "") +
                        "cacheName=" + cacheName + ", " +
                        "typeName=" + typeName + ", " +
                        "affKeyFieldName1=" + oldFieldName + ", " +
                        "affKeyFieldName2=" + fieldName + "].";

                    throwIgniteCheckedException(log, fail, msg);
                }
            }
        }

        return keyConfigurations;
    }

    /**
     * If -DIGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK=true then output message to log else throw IgniteCheckedException.
     *
     * @param log Logger used to log warning message (used only if fail flag is not set).
     * @param fail If true throws IgniteCheckedException in case of attribute values mismatch, otherwise logs warning.
     * @param msg Message to output.
     * @throws IgniteCheckedException
     */
    private static void throwIgniteCheckedException(IgniteLogger log,
        boolean fail, String msg) throws IgniteCheckedException {
        if (fail)
            throw new IgniteCheckedException(msg + " Fix cache configuration or set system property -D" +
                IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK + "=true.");
        else {
            assert log != null;

            U.warn(log, msg);
        }
    }

    /**
     * Validate and compare affinity key configurations.
     *
     * @param groupName Cache group name.
     * @param cacheName Cache name.
     * @param rmtNodeId Remote node.
     * @param rmtCacheKeyCfgs Exist affinity key configurations.
     * @param locCacheKeyCfgs New affinity key configurations.
     * @param log Logger used to log warning message (used only if fail flag is not set).
     * @param fail If true throws IgniteCheckedException in case of attribute values mismatch, otherwise logs warning.
     * @throws IgniteCheckedException In case the affinity key configurations is not valid.
     */
    public static void validateKeyConfigiration(
        String groupName,
        String cacheName,
        UUID rmtNodeId,
        CacheKeyConfiguration rmtCacheKeyCfgs[],
        CacheKeyConfiguration locCacheKeyCfgs[],
        IgniteLogger log,
        boolean fail
    ) throws IgniteCheckedException {
        Map<String, String> rmtAffinityKeys = CU.validateKeyConfigiration(groupName, cacheName, rmtCacheKeyCfgs, log, fail);

        Map<String, String> locAffinityKey = CU.validateKeyConfigiration(groupName, cacheName, locCacheKeyCfgs, log, fail);

        if (rmtAffinityKeys.size() != locAffinityKey.size()) {
            throwIgniteCheckedException(log, fail, "Affinity key configuration mismatch" +
                "[" +
                (groupName != null ? "cacheGroup=" + groupName + ", " : "") +
                "cacheName=" + cacheName + ", " +
                "remote keyConfiguration.length=" + rmtAffinityKeys.size() + ", " +
                "local keyConfiguration.length=" + locAffinityKey.size() +
                (rmtNodeId != null ? ", rmtNodeId=" + rmtNodeId : "") +
                ']');
        }

        for (Map.Entry<String, String> rmtAffinityKey : rmtAffinityKeys.entrySet()) {
            String rmtTypeName = rmtAffinityKey.getKey();

            String rmtFieldName = rmtAffinityKey.getValue();

            String locFieldName = locAffinityKey.get(rmtTypeName);

            if (!rmtFieldName.equals(locFieldName)) {
                throwIgniteCheckedException(log, fail, "Affinity key configuration mismatch [" +
                    (groupName != null ? "cacheGroup=" + groupName + ", " : "") +
                    "cacheName=" + cacheName + ", " +
                    "typeName=" + rmtTypeName + ", " +
                    "remote affinityKeyFieldName=" + rmtFieldName + ", " +
                    "local affinityKeyFieldName=" + locFieldName +
                    (rmtNodeId != null ? ", rmtNodeId=" + rmtNodeId : "") +
                    ']');
            }
        }
    }

    /**
     * @param cfg Initializes cache configuration with proper defaults.
     * @param cacheObjCtx Cache object context.
     * @throws IgniteCheckedException If configuration is not valid.
     */
    public static void initializeConfigDefaults(IgniteLogger log, CacheConfiguration cfg,
        CacheObjectContext cacheObjCtx)
        throws IgniteCheckedException {
        if (cfg.getCacheMode() == null)
            cfg.setCacheMode(DFLT_CACHE_MODE);

        if (cfg.getNodeFilter() == null)
            cfg.setNodeFilter(CacheConfiguration.ALL_NODES);

        if (cfg.getAffinity() == null) {
            if (cfg.getCacheMode() == PARTITIONED) {
                RendezvousAffinityFunction aff = new RendezvousAffinityFunction();

                cfg.setAffinity(aff);
            }
            else if (cfg.getCacheMode() == REPLICATED) {
                RendezvousAffinityFunction aff = new RendezvousAffinityFunction(false, 512);

                cfg.setAffinity(aff);

                cfg.setBackups(Integer.MAX_VALUE);
            }
            else
                cfg.setAffinity(new LocalAffinityFunction());
        }
        else {
            if (cfg.getCacheMode() == LOCAL && !(cfg.getAffinity() instanceof LocalAffinityFunction)) {
                cfg.setAffinity(new LocalAffinityFunction());

                U.warn(log, "AffinityFunction configuration parameter will be ignored for local cache" +
                    " [cacheName=" + U.maskName(cfg.getName()) + ']');
            }
        }

        validateKeyConfigiration(cfg.getGroupName(), cfg.getName(), cfg.getKeyConfiguration(), log, true);

        if (cfg.getCacheMode() == REPLICATED)
            cfg.setBackups(Integer.MAX_VALUE);

        if (cfg.getQueryParallelism() > 1 && cfg.getCacheMode() != PARTITIONED)
            throw new IgniteCheckedException("Segmented indices are supported for PARTITIONED mode only.");

        if (cfg.getAffinityMapper() == null)
            cfg.setAffinityMapper(cacheObjCtx.defaultAffMapper());

        if (cfg.getRebalanceMode() == null)
            cfg.setRebalanceMode(ASYNC);

        if (cfg.getAtomicityMode() == null)
            cfg.setAtomicityMode(CacheConfiguration.DFLT_CACHE_ATOMICITY_MODE);

        if (cfg.getWriteSynchronizationMode() == null)
            cfg.setWriteSynchronizationMode(PRIMARY_SYNC);

        assert cfg.getWriteSynchronizationMode() != null;

        if (cfg.getCacheStoreFactory() == null) {
            Factory<CacheLoader> ldrFactory = cfg.getCacheLoaderFactory();
            Factory<CacheWriter> writerFactory = cfg.isWriteThrough() ? cfg.getCacheWriterFactory() : null;

            if (ldrFactory != null || writerFactory != null)
                cfg.setCacheStoreFactory(new GridCacheLoaderWriterStoreFactory(ldrFactory, writerFactory));
        }
        else {
            if (cfg.getCacheLoaderFactory() != null)
                throw new IgniteCheckedException("Cannot set both cache loaded factory and cache store factory " +
                    "for cache: " + U.maskName(cfg.getName()));

            if (cfg.getCacheWriterFactory() != null)
                throw new IgniteCheckedException("Cannot set both cache writer factory and cache store factory " +
                    "for cache: " + U.maskName(cfg.getName()));
        }

        Collection<QueryEntity> entities = cfg.getQueryEntities();

        if (!F.isEmpty(entities))
            cfg.clearQueryEntities().setQueryEntities(QueryUtils.normalizeQueryEntities(entities, cfg));
    }

    /**
     * Creates closure that saves initial value to backup partition.
     * <p>
     * Useful only when store with readThrough is used. In situation when
     * get() on backup node returns successful result, it's expected that
     * localPeek() will be successful as well. But it isn't true when
     * primary node loaded value from local store, in this case backups
     * will remain non-initialized.
     * <br>
     * To meet that requirement the value requested from primary should
     * be saved on backup during get().
     * </p>
     *
     * @param topVer Topology version.
     * @param log Logger.
     * @param cctx Cache context.
     * @param key Key.
     * @param expiryPlc Expiry policy.
     * @param readThrough Read through.
     * @param skipVals Skip values.
     */
    @Nullable public static BackupPostProcessingClosure createBackupPostProcessingClosure(
        final AffinityTopologyVersion topVer,
        final IgniteLogger log,
        final GridCacheContext cctx,
        @Nullable final KeyCacheObject key,
        @Nullable final IgniteCacheExpiryPolicy expiryPlc,
        boolean readThrough,
        boolean skipVals
    ) {
        if (cctx.mvccEnabled() || !readThrough || skipVals ||
            (key != null && !cctx.affinity().backupsByKey(key, topVer).contains(cctx.localNode())))
            return null;

        return new BackupPostProcessingClosure() {
            private void process(KeyCacheObject key, CacheObject val, GridCacheVersion ver, GridDhtCacheAdapter colocated) {
                while (true) {
                    GridCacheEntryEx entry = null;

                    cctx.shared().database().checkpointReadLock();

                    try {
                        entry = colocated.entryEx(key, topVer);

                        entry.initialValue(
                            val,
                            ver,
                            expiryPlc == null ? 0 : expiryPlc.forCreate(),
                            expiryPlc == null ? 0 : toExpireTime(expiryPlc.forCreate()),
                            true,
                            topVer,
                            GridDrType.DR_BACKUP,
                            true);

                        break;
                    }
                    catch (GridCacheEntryRemovedException ignore) {
                        if (log.isDebugEnabled())
                            log.debug("Got removed entry during postprocessing (will retry): " +
                                entry);
                    }
                    catch (IgniteCheckedException e) {
                        U.error(log, "Error saving backup value: " + entry, e);

                        throw new GridClosureException(e);
                    }
                    catch (GridDhtInvalidPartitionException ignored) {
                        break;
                    }
                    finally {
                        if (entry != null)
                            entry.touch();

                        cctx.shared().database().checkpointReadUnlock();
                    }
                }
            }

            @Override public void apply(CacheObject val, GridCacheVersion ver) {
                process(key, val, ver, cctx.dht());
            }

            @Override public void apply(Collection<GridCacheEntryInfo> infos) {
                if (!F.isEmpty(infos)) {
                    GridCacheAffinityManager aff = cctx.affinity();
                    ClusterNode locNode = cctx.localNode();

                    GridDhtCacheAdapter colocated = cctx.cache().isNear()
                        ? ((GridNearCacheAdapter)cctx.cache()).dht()
                        : cctx.dht();

                    for (GridCacheEntryInfo info : infos) {
                        // Save backup value.
                        if (aff.backupsByKey(info.key(), topVer).contains(locNode))
                            process(info.key(), info.value(), info.version(), colocated);
                    }
                }
            }
        };
    }

    /**
     * Checks if cache configuration belongs to persistent cache.
     *
     * @param ccfg Cache configuration.
     * @param dsCfg Data storage config.
     */
    public static boolean isPersistentCache(CacheConfiguration ccfg, DataStorageConfiguration dsCfg) {
        if (dsCfg == null)
            return false;

        // Special handling for system cache is needed.
        if (isSystemCache(ccfg.getName()) || isIgfsCacheInSystemRegion(ccfg)) {
            if (dsCfg.getDefaultDataRegionConfiguration().isPersistenceEnabled())
                return true;

            if (dsCfg.getDataRegionConfigurations() != null) {
                for (DataRegionConfiguration drConf : dsCfg.getDataRegionConfigurations()) {
                    if (drConf.isPersistenceEnabled())
                        return true;
                }
            }

            return false;
        }

        String regName = ccfg.getDataRegionName();

        if (regName == null || regName.equals(dsCfg.getDefaultDataRegionConfiguration().getName()))
            return dsCfg.getDefaultDataRegionConfiguration().isPersistenceEnabled();

        if (dsCfg.getDataRegionConfigurations() != null) {
            for (DataRegionConfiguration drConf : dsCfg.getDataRegionConfigurations()) {
                if (regName.equals(drConf.getName()))
                    return drConf.isPersistenceEnabled();
            }
        }

        return false;
    }

    /**
     * Checks whether cache configuration represents IGFS cache that will be placed in system memory region.
     *
     * @param ccfg Cache config.
     */
    private static boolean isIgfsCacheInSystemRegion(CacheConfiguration ccfg) {
        return IgfsUtils.matchIgfsCacheName(ccfg.getName()) &&
            (SYSTEM_DATA_REGION_NAME.equals(ccfg.getDataRegionName()) || ccfg.getDataRegionName() == null);
    }

    /**
     * @param nodes Nodes to check.
     * @param marshaller JdkMarshaller
     * @param clsLdr Class loader.
     * @return {@code true} if cluster has only in-memory nodes.
     */
    public static boolean isInMemoryCluster(Collection<ClusterNode> nodes, JdkMarshaller marshaller, ClassLoader clsLdr) {
        return nodes.stream().allMatch(serNode -> !CU.isPersistenceEnabled(extractDataStorage(serNode, marshaller, clsLdr)));
    }

    /**
     * Extract and unmarshal data storage configuration from given node.
     *
     * @param node Source of data storage configuration.
     * @return  Data storage configuration for given node,
     * or {@code null} if this node has not data storage configuration.
     */
    @Nullable public static DataStorageConfiguration extractDataStorage(ClusterNode node, JdkMarshaller marshaller, ClassLoader clsLdr) {
        Object dsCfgBytes = node.attribute(IgniteNodeAttributes.ATTR_DATA_STORAGE_CONFIG);

        if (dsCfgBytes instanceof byte[]) {
            try {
                return marshaller.unmarshal((byte[])dsCfgBytes, clsLdr);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }

        return null;
    }

    /**
     * @return {@code true} if persistence is enabled for a default data region, {@code false} if not.
     */
    public static boolean isDefaultDataRegionPersistent(DataStorageConfiguration cfg) {
        if (cfg == null)
            return false;

        DataRegionConfiguration dfltRegionCfg = cfg.getDefaultDataRegionConfiguration();

        if (dfltRegionCfg == null)
            return false;

        return dfltRegionCfg.isPersistenceEnabled();
    }

    /**
     * @return {@code true} if persistence is enabled for at least one data region, {@code false} if not.
     */
    public static boolean isPersistenceEnabled(IgniteConfiguration cfg) {
        return isPersistenceEnabled(cfg.getDataStorageConfiguration());
    }

    /**
     * @return {@code true} if persistence is enabled for at least one data region, {@code false} if not.
     */
    public static boolean isPersistenceEnabled(DataStorageConfiguration cfg) {
        if (cfg == null)
            return false;

        DataRegionConfiguration dfltReg = cfg.getDefaultDataRegionConfiguration();

        if (dfltReg == null)
            return false;

        if (dfltReg.isPersistenceEnabled())
            return true;

        DataRegionConfiguration[] regCfgs = cfg.getDataRegionConfigurations();

        if (regCfgs == null)
            return false;

        for (DataRegionConfiguration regCfg : regCfgs) {
            if (regCfg.isPersistenceEnabled())
                return true;
        }

        return false;
    }

    /**
     * @param pageSize Page size.
     * @param encSpi Encryption spi.
     * @return Page size without encryption overhead.
     */
    public static int encryptedPageSize(int pageSize, EncryptionSpi encSpi) {
        return pageSize
            - (encSpi.encryptedSizeNoPadding(pageSize) - pageSize)
            - encSpi.blockSize(); /* For CRC. */
    }

    /**
     * @param sctx Shared context.
     * @param cacheIds Cache ids.
     * @return First partitioned cache or {@code null} in case no partitioned cache ids are in list.
     */
    public static GridCacheContext<?, ?> firstPartitioned(GridCacheSharedContext<?, ?> sctx, int[] cacheIds) {
        for (int i = 0; i < cacheIds.length; i++) {
            GridCacheContext<?, ?> cctx = sctx.cacheContext(cacheIds[i]);

            if (cctx == null)
                throw new CacheException("Failed to find cache.");

            if (!cctx.isLocal() && !cctx.isReplicated())
                return cctx;
        }

        return null;
    }

    /**
     * @param sctx Shared context.
     * @param cacheIds Cache ids.
     * @return First partitioned cache or {@code null} in case no partitioned cache ids are in list.
     */
    public static GridCacheContext<?, ?> firstPartitioned(GridCacheSharedContext<?, ?> sctx, Iterable<Integer> cacheIds) {
        for (Integer i : cacheIds) {
            GridCacheContext<?, ?> cctx = sctx.cacheContext(i);

            if (cctx == null)
                throw new CacheException("Failed to find cache.");

            if (!cctx.isLocal() && !cctx.isReplicated())
                return cctx;
        }

        return null;
    }

    /**
     * @param cacheName Name of cache or cache template.
     * @return {@code true} if cache name ends with asterisk (*), and therefire is a template name.
     */
    public static boolean isCacheTemplateName(String cacheName) {
        return cacheName.endsWith("*");
    }

    /**
     *
     */
    public interface BackupPostProcessingClosure extends IgniteInClosure<Collection<GridCacheEntryInfo>>,
        IgniteBiInClosure<CacheObject, GridCacheVersion>{
    }
}
