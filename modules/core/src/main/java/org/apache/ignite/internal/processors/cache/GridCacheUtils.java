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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.configuration.Factory;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheAtomicUpdateTimeoutException;
import org.apache.ignite.cache.CachePartialUpdateException;
import org.apache.ignite.cache.CacheServerNotFoundException;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.store.CacheStoreSessionListener;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteClientDisconnectedCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.cluster.ClusterTopologyServerNotFoundException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedLockCancelledException;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionEx;
import org.apache.ignite.internal.transactions.IgniteTxRollbackCheckedException;
import org.apache.ignite.internal.util.lang.IgniteInClosureX;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.P1;
import org.apache.ignite.internal.util.typedef.P2;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteReducer;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.apache.ignite.plugin.CachePluginConfiguration;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;
import org.jsr166.ConcurrentLinkedDeque8;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.GridTopic.TOPIC_REPLICATION;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.READ;

/**
 * Cache utility methods.
 */
public class GridCacheUtils {
    /**  Hadoop syste cache name. */
    public static final String SYS_CACHE_HADOOP_MR = "ignite-hadoop-mr-sys-cache";

    /** System cache name. */
    public static final String UTILITY_CACHE_NAME = "ignite-sys-cache";

    /** Atomics system cache name. */
    public static final String ATOMICS_CACHE_NAME = "ignite-atomics-sys-cache";

    /** Marshaller system cache name. */
    public static final String MARSH_CACHE_NAME = "ignite-marshaller-sys-cache";

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

    /** Skip store flag bit mask. */
    public static final int SKIP_STORE_FLAG_MASK = 0x1;

    /** Empty predicate array. */
    private static final IgnitePredicate[] EMPTY = new IgnitePredicate[0];

    /** Partition to state transformer. */
    private static final IgniteClosure PART2STATE =
        new C1<GridDhtLocalPartition, GridDhtPartitionState>() {
            @Override public GridDhtPartitionState apply(GridDhtLocalPartition p) {
                return p.state();
            }
        };

    /** */
    private static final IgniteClosure<Integer, GridCacheVersion[]> VER_ARR_FACTORY =
        new C1<Integer, GridCacheVersion[]>() {
            @Override public GridCacheVersion[] apply(Integer size) {
                return new GridCacheVersion[size];
            }
        };

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
    private static final CacheEntryPredicate ALWAYS_TRUE0 = new CacheEntrySerializablePredicate(
        new CacheEntryPredicateAdapter() {
            @Override public boolean apply(GridCacheEntryEx e) {
                return true;
            }
        }
    );

    /** */
    private static final CacheEntryPredicate[] ALWAYS_FALSE0_ARR = new CacheEntryPredicate[] {ALWAYS_FALSE0};

    /** Read filter. */
    private static final IgnitePredicate READ_FILTER = new P1<Object>() {
        @Override public boolean apply(Object e) {
            return ((IgniteTxEntry)e).op() == READ;
        }

        @Override public String toString() {
            return "Cache transaction read filter";
        }
    };

    /** Write filter. */
    private static final IgnitePredicate WRITE_FILTER = new P1<Object>() {
        @Override public boolean apply(Object e) {
            return ((IgniteTxEntry)e).op() != READ;
        }

        @Override public String toString() {
            return "Cache transaction write filter";
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
     * @param msg Message to check.
     * @return {@code True} if preloader message.
     */
    public static boolean allowForStartup(Object msg) {
        return ((GridCacheMessage)msg).allowForStartup();
    }

    /**
     * Writes {@link GridCacheVersion} to output stream. This method is meant to be used by
     * implementations of {@link Externalizable} interface.
     *
     * @param out Output stream.
     * @param ver Version to write.
     * @throws IOException If write failed.
     */
    public static void writeVersion(ObjectOutput out, GridCacheVersion ver) throws IOException {
        // Write null flag.
        out.writeBoolean(ver == null);

        if (ver != null) {
            out.writeBoolean(ver instanceof GridCacheVersionEx);

            ver.writeExternal(out);
        }
    }

    /**
     * Reads {@link GridCacheVersion} from input stream. This method is meant to be used by
     * implementations of {@link Externalizable} interface.
     *
     * @param in Input stream.
     * @return Read version.
     * @throws IOException If read failed.
     */
    @Nullable public static GridCacheVersion readVersion(ObjectInput in) throws IOException {
        // If UUID is not null.
        if (!in.readBoolean()) {
            GridCacheVersion ver = in.readBoolean() ? new GridCacheVersionEx() : new GridCacheVersion();

            ver.readExternal(in);

            return ver;
        }

        return null;
    }

    /**
     * @param ctx Cache context.
     * @param meta Meta name.
     * @return Filter for entries with meta.
     */
    public static IgnitePredicate<KeyCacheObject> keyHasMeta(final GridCacheContext ctx, final int meta) {
        return new P1<KeyCacheObject>() {
            @Override public boolean apply(KeyCacheObject k) {
                GridCacheEntryEx e = ctx.cache().peekEx(k);

                return e != null && e.hasMeta(meta);
            }
        };
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
     * Entry predicate factory mostly used for deserialization.
     *
     * @param <K> Key type.
     * @param <V> Value type.
     * @return Factory instance.
     */
    public static <K, V> IgniteClosure<Integer, IgnitePredicate<Cache.Entry<K, V>>[]> factory() {
        return new IgniteClosure<Integer, IgnitePredicate<Cache.Entry<K, V>>[]>() {
            @SuppressWarnings({"unchecked"})
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
     * @param ctx Cache registry.
     * @return Space name.
     */
    public static String swapSpaceName(GridCacheContext<?, ?> ctx) {
        String name = ctx.namex();

        name = name == null ? "gg-swap-cache-dflt" : "gg-swap-cache-" + name;

        return name;
    }

    /**
     * @param swapSpaceName Swap space name.
     * @return Cache name.
     */
    public static String cacheNameForSwapSpaceName(String swapSpaceName) {
        assert swapSpaceName != null;

        return "gg-swap-cache-dflt".equals(swapSpaceName) ? null : swapSpaceName.substring("gg-swap-cache-".length());
    }

    /**
     * Gets public cache name substituting null name by {@code 'default'}.
     *
     * @return Public cache name substituting null name by {@code 'default'}.
     */
    public static String namexx(@Nullable String name) {
        return name == null ? "default" : name;
    }

    /**
     * @return Partition to state transformer.
     */
    @SuppressWarnings({"unchecked"})
    public static IgniteClosure<GridDhtLocalPartition, GridDhtPartitionState> part2state() {
        return PART2STATE;
    }

    /**
     * Gets all nodes on which cache with the same name is started.
     *
     * @param ctx Cache context.
     * @param topOrder Maximum allowed node order.
     * @return All nodes on which cache with the same name is started (including nodes
     *      that may have already left).
     */
    public static Collection<ClusterNode> allNodes(GridCacheContext ctx, AffinityTopologyVersion topOrder) {
        return ctx.discovery().cacheNodes(ctx.namex(), topOrder);
    }

    /**
     * Gets all nodes with at least one cache configured.
     *
     * @param ctx Shared cache context.
     * @param topOrder Maximum allowed node order.
     * @return All nodes on which cache with the same name is started (including nodes
     *      that may have already left).
     */
    public static Collection<ClusterNode> allNodes(GridCacheSharedContext ctx, AffinityTopologyVersion topOrder) {
        return ctx.discovery().cacheNodes(topOrder);
    }

    /**
     * Gets remote nodes with at least one cache configured.
     *
     * @param ctx Cache shared context.
     * @param topVer Topology version.
     * @return Collection of remote nodes with at least one cache configured.
     */
    public static Collection<ClusterNode> remoteNodes(final GridCacheSharedContext ctx, AffinityTopologyVersion topVer) {
        return ctx.discovery().remoteCacheNodes(topVer);
    }

    /**
     * Gets alive remote nodes with at least one cache configured.
     *
     * @param ctx Cache context.
     * @param topOrder Maximum allowed node order.
     * @return Affinity nodes.
     */
    public static Collection<ClusterNode> aliveRemoteServerNodesWithCaches(final GridCacheSharedContext ctx,
        AffinityTopologyVersion topOrder) {
        return ctx.discovery().aliveRemoteServerNodesWithCaches(topOrder);
    }

    /**
     * Gets all nodes on which cache with the same name is started and the local DHT storage is enabled.
     *
     * @param ctx Cache context.
     * @return All nodes on which cache with the same name is started.
     */
    public static Collection<ClusterNode> affinityNodes(final GridCacheContext ctx) {
        return ctx.discovery().cacheAffinityNodes(ctx.namex(), AffinityTopologyVersion.NONE);
    }

    /**
     * Gets DHT affinity nodes.
     *
     * @param ctx Cache context.
     * @param topOrder Maximum allowed node order.
     * @return Affinity nodes.
     */
    public static Collection<ClusterNode> affinityNodes(GridCacheContext ctx, AffinityTopologyVersion topOrder) {
        return ctx.discovery().cacheAffinityNodes(ctx.namex(), topOrder);
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
     * Gets oldest alive server node with at least one cache configured for specified topology version.
     *
     * @param ctx Context.
     * @param topVer Maximum allowed topology version.
     * @return Oldest alive cache server node.
     */
    @Nullable public static ClusterNode oldestAliveCacheServerNode(GridCacheSharedContext ctx,
        AffinityTopologyVersion topVer) {
        Collection<ClusterNode> nodes = ctx.discovery().aliveServerNodesWithCaches(topVer);

        if (nodes.isEmpty())
            return null;

        return oldest(nodes);
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
    @SuppressWarnings({"unchecked"})
    public static <K, V> IgnitePredicate<Cache.Entry<K, V>>[] empty() {
        return (IgnitePredicate<Cache.Entry<K, V>>[])EMPTY_FILTER;
    }

    /**
     * @return Empty filter.
     */
    @SuppressWarnings({"unchecked"})
    public static CacheEntryPredicate[] empty0() {
        return EMPTY_FILTER0;
    }

    /**
     * @return Always false filter.
     */
    public static CacheEntryPredicate alwaysFalse0() {
        return ALWAYS_FALSE0;
    }

    /**
     * @return Always false filter.
     */
    public static CacheEntryPredicate alwaysTrue0() {
        return ALWAYS_TRUE0;
    }

    /**
     * @return Always false filter.
     */
    public static CacheEntryPredicate[] alwaysFalse0Arr() {
        return ALWAYS_FALSE0_ARR;
    }

    /**
     * @param p Predicate.
     * @return {@code True} if always false filter.
     */
    public static boolean isAlwaysFalse0(@Nullable CacheEntryPredicate[] p) {
        return p != null && p.length == 1 && p[0]  == ALWAYS_FALSE0;
    }

    /**
     * @param p Predicate.
     * @return {@code True} if always false filter.
     */
    public static boolean isAlwaysTrue0(@Nullable CacheEntryPredicate[] p) {
        return p != null && p.length == 1 && p[0]  == ALWAYS_TRUE0;
    }

    /**
     * @return Closure which converts transaction entry xid to XID version.
     */
    @SuppressWarnings( {"unchecked"})
    public static <K, V> IgniteClosure<IgniteInternalTx, GridCacheVersion> tx2xidVersion() {
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
    @SuppressWarnings({"unchecked"})
    public static <K, V> IgniteClosure<GridCacheEntryInfo, K> info2Key() {
        return (IgniteClosure<GridCacheEntryInfo, K>)info2key;
    }

    /**
     * @return Filter for transaction reads.
     */
    @SuppressWarnings({"unchecked"})
    public static <K, V> IgnitePredicate<IgniteTxEntry> reads() {
        return READ_FILTER;
    }

    /**
     * @return Filter for transaction writes.
     */
    @SuppressWarnings({"unchecked"})
    public static <K, V> IgnitePredicate<IgniteTxEntry> writes() {
        return WRITE_FILTER;
    }

    /**
     * Gets type filter for projections.
     *
     * @param keyType Key type.
     * @param valType Value type.
     * @param <K> Key type.
     * @param <V> Value type.
     * @return Type filter.
     */
    public static <K, V> IgniteBiPredicate<K, V> typeFilter(final Class<?> keyType, final Class<?> valType) {
        return new P2<K, V>() {
            @Override public boolean apply(K k, V v) {
                return keyType.isAssignableFrom(k.getClass()) && valType.isAssignableFrom(v.getClass());
            }

            @Override public String toString() {
                return "Type filter [keyType=" + keyType + ", valType=" + valType + ']';
            }
        };
    }

    /**
     * @param keyType Key type.
     * @param valType Value type.
     * @return Type filter.
     */
    public static CacheEntryPredicate typeFilter0(final Class<?> keyType, final Class<?> valType) {
        return new CacheEntrySerializablePredicate(new CacheEntryPredicateAdapter() {
            @Override public boolean apply(GridCacheEntryEx e) {
                Object val = CU.value(peekVisibleValue(e), e.context(), false);

                return val == null ||
                    valType.isAssignableFrom(val.getClass()) &&
                    keyType.isAssignableFrom(e.key().value(e.context().cacheObjectContext(), false).getClass());
            }
        });
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
     * Gets reducer that aggregates maps into one.
     *
     * @param size Predicted size of the resulting map to avoid resizings.
     * @param <K> Key type.
     * @param <V> Value type.
     * @return Reducer.
     */
    public static <K, V> IgniteReducer<Map<K, V>, Map<K, V>> mapsReducer(final int size) {
        return new IgniteReducer<Map<K, V>, Map<K, V>>() {
            private final Map<K, V> ret = new ConcurrentHashMap8<>(size);

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
    public static <T> IgniteReducer<Collection<T>, Collection<T>> collectionsReducer() {
        return new IgniteReducer<Collection<T>, Collection<T>>() {
            private final Collection<T> ret = new ConcurrentLinkedQueue<>();

            @Override public boolean collect(Collection<T> c) {
                if (c != null)
                    ret.addAll(c);

                return true;
            }

            @Override public Collection<T> reduce() {
                return ret;
            }

            /** {@inheritDoc} */
            @Override public String toString() {
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
     * @param locId Local node ID.
     * @return Local node if it is in the list of nodes, or primary node.
     */
    public static ClusterNode localOrPrimary(Iterable<ClusterNode> nodes, UUID locId) {
        assert !F.isEmpty(nodes);

        for (ClusterNode n : nodes)
            if (n.id().equals(locId))
                return n;

        return F.first(nodes);
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
     * @param mappings Mappings.
     * @param k map key.
     * @return Either current list value or newly created one.
     */
    public static <K, V> Collection<V> getOrSet(Map<K, List<V>> mappings, K k) {
        List<V> vals = mappings.get(k);

        if (vals == null)
            mappings.put(k, vals = new LinkedList<>());

        return vals;
    }

    /**
     * @param mappings Mappings.
     * @param k map key.
     * @return Either current list value or newly created one.
     */
    public static <K, V> Collection<V> getOrSet(ConcurrentMap<K, Collection<V>> mappings, K k) {
        Collection<V> vals = mappings.get(k);

        if (vals == null) {
            Collection<V> old = mappings.putIfAbsent(k, vals = new ConcurrentLinkedDeque8<>());

            if (old != null)
                vals = old;
        }

        return vals;
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
     * @return {@code true} if caused by lock timeout.
     */
    public static boolean isLockTimeout(Throwable t) {
        if (t == null)
            return false;

        while (t instanceof IgniteCheckedException || t instanceof IgniteException)
            t = t.getCause();

        return t instanceof GridCacheLockTimeoutException;
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
    @SuppressWarnings("unchecked")
    public static byte[] marshal(GridCacheSharedContext ctx, Object obj)
        throws IgniteCheckedException {
        assert ctx != null;

        if (ctx.gridDeploy().enabled()) {
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

        return ctx.marshaller().marshal(obj);
    }

    /**
     * Method executes any Callable out of scope of transaction.
     * If transaction started by this thread {@code cmd} will be executed in another thread.
     *
     * @param cmd Callable.
     * @param ctx Cache context.
     * @return T Callable result.
     * @throws IgniteCheckedException If execution failed.
     */
    public static <T> T outTx(Callable<T> cmd, GridCacheContext ctx) throws IgniteCheckedException {
        if (ctx.tm().inUserTx())
            return ctx.closures().callLocalSafe(cmd, false).get();
        else {
            try {
                return cmd.call();
            }
            catch (IgniteCheckedException | IgniteException | IllegalStateException e) {
                throw e;
            }
            catch (Exception e) {
                throw new IgniteCheckedException(e);
            }
        }
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
    public static IgniteInternalTx txStartInternal(GridCacheContext ctx, IgniteInternalCache prj,
        TransactionConcurrency concurrency, TransactionIsolation isolation) {
        assert ctx != null;
        assert prj != null;

        ctx.tm().resetContext();

        return prj.txStartEx(concurrency, isolation);
    }

    /**
     * @param tx Transaction.
     * @return String view of all safe-to-print transaction properties.
     */
    public static String txString(@Nullable IgniteInternalTx tx) {
        if (tx == null)
            return "null";

        return tx.getClass().getSimpleName() + "[id=" + tx.xid() + ", concurrency=" + tx.concurrency() +
            ", isolation=" + tx.isolation() + ", state=" + tx.state() + ", invalidate=" + tx.isInvalidate() +
            ", rollbackOnly=" + tx.isRollbackOnly() + ", nodeId=" + tx.nodeId() +
            ", duration=" + (U.currentTimeMillis() - tx.startTime()) + ']';
    }

    /**
     * @param ctx Cache context.
     */
    public static void unwindEvicts(GridCacheContext ctx) {
        assert ctx != null;

        ctx.evicts().unwind();

        if (ctx.isNear())
            ctx.near().dht().context().evicts().unwind();

        ctx.ttl().expire();
    }

    /**
     * @param ctx Shared cache context.
     */
    public static <K, V> void unwindEvicts(GridCacheSharedContext<K, V> ctx) {
        assert ctx != null;

        for (GridCacheContext<K, V> cacheCtx : ctx.cacheContexts()) {
            cacheCtx.evicts().unwind();

            if (cacheCtx.isNear())
                cacheCtx.near().dht().context().evicts().unwind();

            cacheCtx.ttl().expire();
        }
    }

    /**
     * Gets primary node on which given key is cached.
     *
     * @param ctx Cache.
     * @param key Key to find primary node for.
     * @return Primary node for the key.
     */
    @SuppressWarnings( {"unchecked"})
    @Nullable public static ClusterNode primaryNode(GridCacheContext ctx, Object key) {
        assert ctx != null;
        assert key != null;

        CacheConfiguration cfg = ctx.cache().configuration();

        if (cfg.getCacheMode() != PARTITIONED)
            return ctx.localNode();

        return ctx.affinity().primary(key, ctx.affinity().affinityTopologyVersion());
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
     * @return Version array factory.
     */
    public static IgniteClosure<Integer, GridCacheVersion[]> versionArrayFactory() {
        return VER_ARR_FACTORY;
    }

    /**
     * Converts cache version to byte array.
     *
     * @param ver Version.
     * @return Byte array.
     */
    public static byte[] versionToBytes(GridCacheVersion ver) {
        assert ver != null;

        byte[] bytes = new byte[28];

        U.intToBytes(ver.topologyVersion(), bytes, 0);
        U.longToBytes(ver.globalTime(), bytes, 4);
        U.longToBytes(ver.order(), bytes, 12);
        U.intToBytes(ver.nodeOrderAndDrIdRaw(), bytes, 20);

        return bytes;
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

        if (!F.eq(locVal, rmtVal)) {
            if (fail) {
                throw new IgniteCheckedException(attrMsg + " mismatch (fix " + attrMsg.toLowerCase() + " in cache " +
                    "configuration or set -D" + IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK + "=true " +
                    "system property) [cacheName=" + cfgName +
                    ", local" + capitalize(attrName) + "=" + locVal +
                    ", remote" + capitalize(attrName) + "=" + rmtVal +
                    ", rmtNodeId=" + rmtNodeId + ']');
            }
            else {
                assert log != null;

                U.warn(log, attrMsg + " mismatch (fix " + attrMsg.toLowerCase() + " in cache " +
                    "configuration) [cacheName=" + cfgName +
                    ", local" + capitalize(attrName) + "=" + locVal +
                    ", remote" + capitalize(attrName) + "=" + rmtVal +
                    ", rmtNodeId=" + rmtNodeId + ']');
            }
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
     * Validates that cache value object implements {@link Externalizable}.
     *
     * @param log Logger used to log warning message.
     * @param val Value.
     */
    public static void validateCacheValue(IgniteLogger log, @Nullable Object val) {
        if (val == null)
            return;

        validateExternalizable(log, val);
    }

    /**
     * Validates that cache key object has overridden equals and hashCode methods and
     * implements {@link Externalizable}.
     *
     * @param log Logger used to log warning message.
     * @param key Key.
     * @throws IllegalArgumentException If equals or hashCode is not implemented.
     */
    public static void validateCacheKey(IgniteLogger log, @Nullable Object key) {
        if (key == null)
            return;

        validateExternalizable(log, key);

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

        cache.setEvictionPolicy(null);
        cache.setSwapEnabled(false);
        cache.setCacheStoreFactory(null);
        cache.setNodeFilter(CacheConfiguration.ALL_NODES);
        cache.setEagerTtl(true);
        cache.setRebalanceMode(SYNC);

        return cache;
    }

    /**
     * @param cacheName Cache name.
     * @return {@code True} if this is marshaller system cache.
     */
    public static boolean isMarshallerCache(String cacheName) {
        return MARSH_CACHE_NAME.equals(cacheName);
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
     * @return {@code True} if this is atomics system cache.
     */
    public static boolean isAtomicsCache(String cacheName) {
        return ATOMICS_CACHE_NAME.equals(cacheName);
    }

    /**
     * @param cacheName Cache name.
     * @return {@code True} if system cache.
     */
    public static boolean isSystemCache(String cacheName) {
        return isMarshallerCache(cacheName) || isUtilityCache(cacheName) || isHadoopSystemCache(cacheName) ||
            isAtomicsCache(cacheName);
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
     * Validates that cache key or cache value implements {@link Externalizable}
     *
     * @param log Logger used to log warning message.
     * @param obj Cache key or cache value.
     */
    private static void validateExternalizable(IgniteLogger log, Object obj) {
        Class<?> cls = obj.getClass();

        if (!cls.isArray() && !U.isJdk(cls) && !(obj instanceof Externalizable) && !(obj instanceof GridCacheInternal))
            LT.warn(log, null, "For best performance you should implement " +
                "java.io.Externalizable for all cache keys and values: " + cls.getName());
    }

//    /**
//     * @param cfg Grid configuration.
//     * @param cacheName Cache name.
//     * @return {@code True} in this is Mongo data or meta cache.
//     */
//    public static boolean isMongoCache(GridConfiguration cfg, @Nullable String cacheName) {
//        GridMongoConfiguration mongoCfg = cfg.getMongoConfiguration();
//
//        if (mongoCfg != null) {
//            if (F.eq(cacheName, mongoCfg.getDefaultDataCacheName()) || F.eq(cacheName, mongoCfg.getMetaCacheName()))
//                return true;
//
//            // Mongo config probably has not been validated yet => possible NPE, so we check for null.
//            if (mongoCfg.getDataCacheNames() != null) {
//                for (String mongoCacheName : mongoCfg.getDataCacheNames().values()) {
//                    if (F.eq(cacheName, mongoCacheName))
//                        return true;
//                }
//            }
//        }
//
//        return false;
//    }

    /**
     * @param cfg Grid configuration.
     * @param cacheName Cache name.
     * @return {@code True} in this is IGFS data or meta cache.
     */
    public static boolean isIgfsCache(IgniteConfiguration cfg, @Nullable String cacheName) {
        FileSystemConfiguration[] igfsCfgs = cfg.getFileSystemConfiguration();

        if (igfsCfgs != null) {
            for (FileSystemConfiguration igfsCfg : igfsCfgs) {
                // IGFS config probably has not been validated yet => possible NPE, so we check for null.
                if (igfsCfg != null &&
                    (F.eq(cacheName, igfsCfg.getDataCacheName()) || F.eq(cacheName, igfsCfg.getMetaCacheName())))
                    return true;
            }
        }

        return false;
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

        try (IgniteInternalTx tx = cache.txStartEx(concurrency, isolation);) {
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
     * Reads array from input stream.
     *
     * @param in Input stream.
     * @return Deserialized array.
     * @throws IOException If failed.
     * @throws ClassNotFoundException If class not found.
     */
    @SuppressWarnings("unchecked")
    @Nullable public static <K, V> CacheEntryPredicate[] readEntryFilterArray(ObjectInput in)
        throws IOException, ClassNotFoundException {
        int len = in.readInt();

        CacheEntryPredicate[] arr = null;

        if (len > 0) {
            arr = new CacheEntryPredicate[len];

            for (int i = 0; i < len; i++)
                arr[i] = (CacheEntryPredicate)in.readObject();
        }

        return arr;
    }

    /**
     * @param aff Affinity.
     * @param n Node.
     * @return Predicate that evaluates to {@code true} if entry is primary for node.
     */
    public static CacheEntryPredicate cachePrimary(
        final Affinity aff,
        final ClusterNode n
    ) {
        return new CacheEntryPredicateAdapter() {
            @Override public boolean apply(GridCacheEntryEx e) {
                return aff.isPrimary(n, e.key().value(e.context().cacheObjectContext(), false));
            }
        };
    }

    /**
     * @param aff Affinity.
     * @param n Node.
     * @return Predicate that evaulates to {@code true} if entry is primary for node.
     */
    public static <K, V> IgnitePredicate<Cache.Entry<K, V>> cachePrimary0(
        final Affinity<K> aff,
        final ClusterNode n
    ) {
        return new IgnitePredicate<Cache.Entry<K, V>>() {
            @Override public boolean apply(Cache.Entry<K, V> e) {
                return aff.isPrimary(n, e.getKey());
            }
        };
    }

    /**
     * @param e Ignite checked exception.
     * @return CacheException runtime exception, never null.
     */
    @NotNull public static RuntimeException convertToCacheException(IgniteCheckedException e) {
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
        else if (e instanceof CacheAtomicUpdateTimeoutCheckedException)
            return new CacheAtomicUpdateTimeoutException(e.getMessage(), e);
        else if (e instanceof ClusterTopologyServerNotFoundException)
            return new CacheServerNotFoundException(e.getMessage(), e);

        if (e.getCause() instanceof CacheException)
            return (CacheException)e.getCause();

        if (e.getCause() instanceof NullPointerException)
            return (NullPointerException)e.getCause();

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
     * @return {@code True} if given node is client node (has flag {@link IgniteConfiguration#isClientMode()} set).
     */
    public static boolean clientNode(ClusterNode node) {
        Boolean clientModeAttr = node.attribute(IgniteNodeAttributes.ATTR_CLIENT_MODE);

        assert clientModeAttr != null : node;

        return clientModeAttr != null && clientModeAttr;
    }

    /**
     * @param node Node.
     * @param filter Node filter.
     * @return {@code True} if node is not client node and pass given filter.
     */
    public static boolean affinityNode(ClusterNode node, IgnitePredicate<ClusterNode> filter) {
        return !clientNode(node) && filter.apply(node);
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

            int[] partsArray = new int[parts.size()];

            int idx = 0;

            for (Integer part : parts)
                partsArray[idx++] = part;

            res.put(entry.getKey(), partsArray);
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
     * @param <S> Closure type.
     * @return Wrapped closure.
     */
    public static <S> Callable<S> retryTopologySafe(final Callable<S> c ) {
        return new Callable<S>() {
            @Override public S call() throws Exception {
                int retries = GridCacheAdapter.MAX_RETRIES;

                IgniteCheckedException err = null;

                for (int i = 0; i < retries; i++) {
                    try {
                        return c.call();
                    }
                    catch (IgniteCheckedException e) {
                        if (X.hasCause(e, ClusterTopologyCheckedException.class) ||
                            X.hasCause(e, IgniteTxRollbackCheckedException.class) ||
                            X.hasCause(e, CachePartialUpdateCheckedException.class)) {
                            if (i < retries - 1) {
                                err = e;

                                U.sleep(1);

                                continue;
                            }

                            throw e;
                        }
                        else
                            throw e;
                    }
                }

                // Should never happen.
                throw err;
            }
        };
    }
}