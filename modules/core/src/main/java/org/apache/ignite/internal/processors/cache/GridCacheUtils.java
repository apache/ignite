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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.cluster.*;
import org.apache.ignite.internal.processors.affinity.*;
import org.apache.ignite.internal.processors.cache.distributed.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.transactions.*;
import org.jetbrains.annotations.*;
import org.jsr166.*;

import javax.cache.*;
import javax.cache.expiry.*;
import javax.cache.integration.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.IgniteSystemProperties.*;
import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.cache.CacheRebalanceMode.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;
import static org.apache.ignite.internal.GridTopic.*;
import static org.apache.ignite.internal.IgniteNodeAttributes.*;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.*;
import static org.apache.ignite.internal.processors.cache.GridCachePeekMode.*;

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

    /** Peek flags. */
    private static final GridCachePeekMode[] PEEK_FLAGS = new GridCachePeekMode[] { GLOBAL, SWAP };

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

    /** Per-thread generated UID store. */
    private static final ThreadLocal<UUID> UUIDS = new ThreadLocal<UUID>() {
        @Override protected UUID initialValue() {
            return UUID.randomUUID();
        }
    };

    /** Empty predicate array. */
    private static final IgnitePredicate[] EMPTY = new IgnitePredicate[0];

    /** Partition to state transformer. */
    private static final IgniteClosure PART2STATE =
        new C1<GridDhtLocalPartition, GridDhtPartitionState>() {
            @Override public GridDhtPartitionState apply(GridDhtLocalPartition p) {
                return p.state();
            }
        };

    /** Not evicted partitions. */
    private static final IgnitePredicate PART_NOT_EVICTED = new P1<GridDhtLocalPartition>() {
        @Override public boolean apply(GridDhtLocalPartition p) {
            return p.state() != GridDhtPartitionState.EVICTED;
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
     * Gets per-thread-unique ID for this thread.
     *
     * @return ID for this thread.
     */
    public static UUID uuid() {
        return UUIDS.get();
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
    public static IgnitePredicate<KeyCacheObject> keyHasMeta(final GridCacheContext ctx, final UUID meta) {
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
     * Gets closure which returns {@code Entry} given cache key.
     * If current cache is DHT and key doesn't belong to current partition,
     * {@code null} is returned.
     *
     * @param ctx Cache context.
     * @param <K> Cache key type.
     * @param <V> Cache value type.
     * @return Closure which returns {@code Entry} given cache key or {@code null} if partition is invalid.
     */
    public static <K, V> IgniteClosure<K, Cache.Entry<K, V>> cacheKey2Entry(
        final GridCacheContext<K, V> ctx) {
        return new IgniteClosure<K, Cache.Entry<K, V>>() {
            @Nullable @Override public Cache.Entry<K, V> apply(K k) {
                try {
                    return ctx.cache().entry(k);
                }
                catch (GridDhtInvalidPartitionException ignored) {
                    return null;
                }
            }

            @Override public String toString() {
                return "Key-to-entry transformer.";
            }
        };
    }

    /**
     * @return Partition to state transformer.
     */
    @SuppressWarnings({"unchecked"})
    public static <K, V> IgniteClosure<GridDhtLocalPartition, GridDhtPartitionState> part2state() {
        return PART2STATE;
    }

    /**
     * @return Not evicted partitions.
     */
    @SuppressWarnings( {"unchecked"})
    public static <K, V> IgnitePredicate<GridDhtLocalPartition> notEvicted() {
        return PART_NOT_EVICTED;
    }

    /**
     * Gets all nodes on which cache with the same name is started.
     *
     * @param ctx Cache context.
     * @return All nodes on which cache with the same name is started (including nodes
     *      that may have already left).
     */
    public static Collection<ClusterNode> allNodes(GridCacheContext ctx) {
        return allNodes(ctx, AffinityTopologyVersion.NONE);
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
     * Gets alive nodes.
     *
     * @param ctx Cache context.
     * @param topOrder Maximum allowed node order.
     * @return Affinity nodes.
     */
    public static Collection<ClusterNode> aliveNodes(final GridCacheContext ctx, AffinityTopologyVersion topOrder) {
        return ctx.discovery().aliveCacheNodes(ctx.namex(), topOrder);
    }

    /**
     * Gets remote nodes on which cache with the same name is started.
     *
     * @param ctx Cache context.
     * @return Remote nodes on which cache with the same name is started.
     */
    public static Collection<ClusterNode> remoteNodes(final GridCacheContext ctx) {
        return remoteNodes(ctx, AffinityTopologyVersion.NONE);
    }

    /**
     * Gets remote node with at least one cache configured.
     *
     * @param ctx Shared cache context.
     * @return Collection of nodes with at least one cache configured.
     */
    public static Collection<ClusterNode> remoteNodes(GridCacheSharedContext ctx) {
        return remoteNodes(ctx, AffinityTopologyVersion.NONE);
    }

    /**
     * Gets remote nodes on which cache with the same name is started.
     *
     * @param ctx Cache context.
     * @param topOrder Maximum allowed node order.
     * @return Remote nodes on which cache with the same name is started.
     */
    public static Collection<ClusterNode> remoteNodes(final GridCacheContext ctx, AffinityTopologyVersion topOrder) {
        return ctx.discovery().remoteCacheNodes(ctx.namex(), topOrder);
    }

    /**
     * Gets alive nodes.
     *
     * @param ctx Cache context.
     * @param topOrder Maximum allowed node order.
     * @return Affinity nodes.
     */
    public static Collection<ClusterNode> aliveRemoteNodes(final GridCacheContext ctx, AffinityTopologyVersion topOrder) {
        return ctx.discovery().aliveRemoteCacheNodes(ctx.namex(), topOrder);
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
     * Gets alive nodes with at least one cache configured.
     *
     * @param ctx Cache context.
     * @param topOrder Maximum allowed node order.
     * @return Affinity nodes.
     */
    public static Collection<ClusterNode> aliveCacheNodes(final GridCacheSharedContext ctx, AffinityTopologyVersion topOrder) {
        return ctx.discovery().aliveNodesWithCaches(topOrder);
    }

    /**
     * Gets alive remote nodes with at least one cache configured.
     *
     * @param ctx Cache context.
     * @param topOrder Maximum allowed node order.
     * @return Affinity nodes.
     */
    public static Collection<ClusterNode> aliveRemoteCacheNodes(final GridCacheSharedContext ctx, AffinityTopologyVersion topOrder) {
        return ctx.discovery().aliveRemoteNodesWithCaches(topOrder);
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
     * Checks if given node has specified cache started.
     *
     * @param cacheName Cache name.
     * @param node Node to check.
     * @return {@code True} if given node has specified cache started.
     */
    public static boolean cacheNode(String cacheName, ClusterNode node) {
        return cacheNode(cacheName, (GridCacheAttributes[])node.attribute(ATTR_CACHE));
    }

    /**
     * Checks if given attributes relate the the node which has (or had) specified cache started.
     *
     * @param cacheName Cache name.
     * @param caches Node cache attributes.
     * @return {@code True} if given node has specified cache started.
     */
    public static boolean cacheNode(String cacheName, GridCacheAttributes[] caches) {
        if (caches != null)
            for (GridCacheAttributes attrs : caches)
                if (F.eq(cacheName, attrs.cacheName()))
                    return true;

        return false;
    }

    /**
     * Gets oldest alive node for specified topology version.
     *
     * @param cctx Cache context.
     * @return Oldest node for the current topology version.
     */
    public static ClusterNode oldest(GridCacheContext cctx) {
        return oldest(cctx, AffinityTopologyVersion.NONE);
    }

    /**
     * Gets oldest alive node across nodes with at least one cache configured.
     *
     * @param ctx Cache context.
     * @return Oldest node.
     */
    public static ClusterNode oldest(GridCacheSharedContext ctx) {
        return oldest(ctx, AffinityTopologyVersion.NONE);
    }

    /**
     * Gets oldest alive node for specified topology version.
     *
     * @param cctx Cache context.
     * @param topOrder Maximum allowed node order.
     * @return Oldest node for the given topology version.
     */
    public static ClusterNode oldest(GridCacheContext cctx, AffinityTopologyVersion topOrder) {
        ClusterNode oldest = null;

        for (ClusterNode n : aliveNodes(cctx, topOrder))
            if (oldest == null || n.order() < oldest.order())
                oldest = n;

        assert oldest != null : "Failed to find oldest node for cache context [name=" + cctx.name() + ", topOrder=" + topOrder + ']';
        assert oldest.order() <= topOrder.topologyVersion() || AffinityTopologyVersion.NONE.equals(topOrder);

        return oldest;
    }

    /**
     * Gets oldest alive node with at least one cache configured for specified topology version.
     *
     * @param cctx Shared cache context.
     * @param topOrder Maximum allowed node order.
     * @return Oldest node for the given topology version.
     */
    public static ClusterNode oldest(GridCacheSharedContext cctx, AffinityTopologyVersion topOrder) {
        ClusterNode oldest = null;

        for (ClusterNode n : aliveCacheNodes(cctx, topOrder)) {
            if (oldest == null || n.order() < oldest.order())
                oldest = n;
        }

        assert oldest != null : "Failed to find oldest node with caches: " + topOrder;
        assert oldest.order() <= topOrder.topologyVersion() || AffinityTopologyVersion.NONE.equals(topOrder);

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
     * @return Closure that converts tx entry to key.
     */
    @SuppressWarnings({"unchecked"})
    public static <K, V> IgniteClosure<IgniteTxEntry, K> tx2key() {
        return (IgniteClosure<IgniteTxEntry, K>)tx2key;
    }

    /**
     * @return Closure that converts tx entry collection to key collection.
     */
    @SuppressWarnings({"unchecked"})
    public static <K, V> IgniteClosure<Collection<IgniteTxEntry>, Collection<K>> txCol2Key() {
        return (IgniteClosure<Collection<IgniteTxEntry>, Collection<K>>)txCol2key;
    }

    /**
     * @return Converts transaction entry to cache entry.
     */
    @SuppressWarnings( {"unchecked"})
    public static <K, V> IgniteClosure<IgniteTxEntry, GridCacheEntryEx> tx2entry() {
        return (IgniteClosure<IgniteTxEntry, GridCacheEntryEx>)tx2entry;
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
     * @return Peek flags.
     */
    public static GridCachePeekMode[] peekFlags() {
        return PEEK_FLAGS;
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
     * @param ctx Context.
     * @param keys Keys.
     * @return Mapped keys.
     */
    @SuppressWarnings( {"unchecked", "MismatchedQueryAndUpdateOfCollection"})
    public static <K> Map<ClusterNode, Collection<K>> mapKeysToNodes(GridCacheContext<K, ?> ctx,
        Collection<? extends K> keys) {
        if (keys == null || keys.isEmpty())
            return Collections.emptyMap();

        // Map all keys to local node for local caches.
        if (ctx.config().getCacheMode() == LOCAL)
            return F.asMap(ctx.localNode(), (Collection<K>)keys);

        AffinityTopologyVersion topVer = new AffinityTopologyVersion(ctx.discovery().topologyVersion());

        if (CU.affinityNodes(ctx, topVer).isEmpty())
            return Collections.emptyMap();

        if (keys.size() == 1)
            return Collections.singletonMap(ctx.affinity().primary(F.first(keys), topVer), (Collection<K>)keys);

        Map<ClusterNode, Collection<K>> map = new GridLeanMap<>(5);

        for (K k : keys) {
            ClusterNode primary = ctx.affinity().primary(k, topVer);

            Collection<K> mapped = map.get(primary);

            if (mapped == null)
                map.put(primary, mapped = new LinkedList<>());

            mapped.add(k);
        }

        return map;
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
    public static IgniteInternalTx txStartInternal(GridCacheContext ctx, CacheProjection prj,
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
    }

    /**
     * @param ctx Shared cache context.
     */
    public static <K, V> void unwindEvicts(GridCacheSharedContext<K, V> ctx) {
        for (GridCacheContext<K, V> cacheCtx : ctx.cacheContexts()) {
            assert ctx != null;

            cacheCtx.evicts().unwind();

            if (cacheCtx.isNear())
                cacheCtx.near().dht().context().evicts().unwind();
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
    public static ClusterNode primaryNode(GridCacheContext ctx, Object key) {
        assert ctx != null;
        assert key != null;

        CacheConfiguration cfg = ctx.cache().configuration();

        if (cfg.getCacheMode() != PARTITIONED)
            return ctx.localNode();

        ClusterNode primary = ctx.affinity().primary(key, ctx.affinity().affinityTopologyVersion());

        assert primary != null;

        return primary;
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
     * @return Cache ID for utility cache.
     */
    public static int utilityCacheId() {
        return cacheId(UTILITY_CACHE_NAME);
    }

    /**
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
    public static <K, V> void inTx(CacheProjection<K, V> cache, TransactionConcurrency concurrency,
        TransactionIsolation isolation, IgniteInClosureX<CacheProjection<K ,V>> clo) throws IgniteCheckedException {

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
     * @return {@code True} if entry was invalidated.
     */
    public static <K, V> boolean invalidate(CacheProjection<K, V> cache, K key) {
        return cache.clearLocally(key);
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
     * @return Predicate that evaulates to {@code true} if entry is primary for node.
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
}
