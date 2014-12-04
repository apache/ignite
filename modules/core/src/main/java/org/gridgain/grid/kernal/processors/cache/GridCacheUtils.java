/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.kernal.processors.cache.distributed.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.GridSystemProperties.*;
import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCachePeekMode.*;
import static org.gridgain.grid.cache.GridCachePreloadMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;
import static org.gridgain.grid.kernal.GridNodeAttributes.*;
import static org.gridgain.grid.kernal.GridTopic.*;
import static org.gridgain.grid.kernal.processors.cache.GridCacheOperation.*;

/**
 * Cache utility methods.
 */
public class GridCacheUtils {
    /**  Hadoop syste cache name. */
    public static final String SYS_CACHE_HADOOP_MR = "gg-hadoop-mr-sys-cache";

    /** Security system cache name. */
    public static final String UTILITY_CACHE_NAME = "gg-sys-cache";

    /** Flag to turn off DHT cache for debugging purposes. */
    public static final boolean DHT_ENABLED = true;

    /** Default mask name. */
    private static final String DEFAULT_MASK_NAME = "<default>";

    /** Peek flags. */
    private static final GridCachePeekMode[] PEEK_FLAGS = new GridCachePeekMode[] { GLOBAL, SWAP };

    /** Per-thread generated UID store. */
    private static final ThreadLocal<String> UUIDS = new ThreadLocal<String>() {
        @Override protected String initialValue() {
            return UUID.randomUUID().toString();
        }
    };

    /** Empty predicate array. */
    private static final GridPredicate[] EMPTY = new GridPredicate[0];

    /** Partition to state transformer. */
    private static final IgniteClosure PART2STATE =
        new C1<GridDhtLocalPartition, GridDhtPartitionState>() {
            @Override public GridDhtPartitionState apply(GridDhtLocalPartition p) {
                return p.state();
            }
        };

    /** Not evicted partitions. */
    private static final GridPredicate PART_NOT_EVICTED = new P1<GridDhtLocalPartition>() {
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
    private static final GridPredicate[] EMPTY_FILTER = new GridPredicate[0];

    /** Always false predicat array. */
    private static final GridPredicate[] ALWAYS_FALSE = new GridPredicate[] {
        new P1() {
            @Override public boolean apply(Object e) {
                return false;
            }
        }
    };

    /** Read filter. */
    private static final GridPredicate READ_FILTER = new P1<Object>() {
        @Override public boolean apply(Object e) {
            return ((GridCacheTxEntry)e).op() == READ;
        }

        @Override public String toString() {
            return "Cache transaction read filter";
        }
    };

    /** Write filter. */
    private static final GridPredicate WRITE_FILTER = new P1<Object>() {
        @Override public boolean apply(Object e) {
            return ((GridCacheTxEntry)e).op() != READ;
        }

        @Override public String toString() {
            return "Cache transaction write filter";
        }
    };

    /** Transfer required predicate. */
    private static final GridPredicate TRANSFER_REQUIRED_PREDICATE = new P1<GridCacheTxEntry>() {
        @Override public boolean apply(GridCacheTxEntry e) {
            return e.transferRequired();
        }
    };

    /** Transaction entry to key. */
    private static final IgniteClosure tx2key = new C1<GridCacheTxEntry, Object>() {
        @Override public Object apply(GridCacheTxEntry e) {
            return e.key();
        }

        @Override public String toString() {
            return "Cache transaction entry to key converter.";
        }
    };

    /** Transaction entry to key. */
    private static final IgniteClosure txCol2key = new C1<Collection<GridCacheTxEntry>, Collection<Object>>() {
        @SuppressWarnings( {"unchecked"})
        @Override public Collection<Object> apply(Collection<GridCacheTxEntry> e) {
            return F.viewReadOnly(e, tx2key);
        }

        @Override public String toString() {
            return "Cache transaction entry collection to key collection converter.";
        }
    };

    /** Converts transaction to XID. */
    private static final IgniteClosure<GridCacheTx, GridUuid> tx2xid = new C1<GridCacheTx, GridUuid>() {
        @Override public GridUuid apply(GridCacheTx tx) {
            return tx.xid();
        }

        @Override public String toString() {
            return "Transaction to XID converter.";
        }
    };

    /** Converts transaction to XID version. */
    private static final IgniteClosure tx2xidVer = new C1<GridCacheTxEx, GridCacheVersion>() {
        @Override public GridCacheVersion apply(GridCacheTxEx tx) {
            return tx.xidVersion();
        }

        @Override public String toString() {
            return "Transaction to XID version converter.";
        }
    };

    /** Converts tx entry to entry. */
    private static final IgniteClosure tx2entry = new C1<GridCacheTxEntry, GridCacheEntryEx>() {
        @Override public GridCacheEntryEx apply(GridCacheTxEntry e) {
            return e.cached();
        }
    };

    /** Transaction entry to key bytes. */
    private static final IgniteClosure tx2keyBytes = new C1<GridCacheTxEntry, byte[]>() {
        @Nullable @Override public byte[] apply(GridCacheTxEntry e) {
            return e.keyBytes();
        }

        @Override public String toString() {
            return "Cache transaction entry to key converter.";
        }
    };

    /** Transaction entry to key. */
    private static final IgniteClosure entry2key = new C1<GridCacheEntryEx, Object>() {
        @Override public Object apply(GridCacheEntryEx e) {
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
    public static String uuid() {
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
     * @param <K> Key type.
     * @param <V> Value type.
     * @return Filter for entries with meta.
     */
    public static <K, V> GridPredicate<K> keyHasMeta(final GridCacheContext<K, V> ctx, final String meta) {
        return new P1<K>() {
            @Override public boolean apply(K k) {
                GridCacheEntryEx<K, V> e = ctx.cache().peekEx(k);

                return e != null && e.hasMeta(meta);
            }
        };
    }

    /**
     * @param err If {@code true}, then throw {@link GridCacheFilterFailedException},
     *      otherwise return {@code val} passed in.
     * @param <T> Return type.
     * @return Always return {@code null}.
     * @throws GridCacheFilterFailedException If {@code err} flag is {@code true}.
     */
    @Nullable public static <T> T failed(boolean err) throws GridCacheFilterFailedException {
        return failed(err, (T)null);
    }

    /**
     * @param err If {@code true}, then throw {@link GridCacheFilterFailedException},
     *      otherwise return {@code val} passed in.
     * @param val Value for which evaluation happened.
     * @param <T> Return type.
     * @return Always return {@code val} passed in or throw exception.
     * @throws GridCacheFilterFailedException If {@code err} flag is {@code true}.
     */
    @Nullable public static <T> T failed(boolean err, T val) throws GridCacheFilterFailedException {
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
    public static <K, V> IgniteClosure<Integer, GridPredicate<GridCacheEntry<K, V>>[]> factory() {
        return new IgniteClosure<Integer, GridPredicate<GridCacheEntry<K, V>>[]>() {
            @SuppressWarnings({"unchecked"})
            @Override public GridPredicate<GridCacheEntry<K, V>>[] apply(Integer len) {
                return (GridPredicate<GridCacheEntry<K, V>>[])(len == 0 ? EMPTY : new GridPredicate[len]);
            }
        };
    }

    /**
     * Checks that cache store is present.
     *
     * @param ctx Registry.
     * @throws GridException If cache store is not present.
     */
    public static void checkStore(GridCacheContext<?, ?> ctx) throws GridException {
        if (!ctx.store().configured())
            throw new GridException("Failed to find cache store for method 'reload(..)' " +
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
     * Gets closure which returns {@link GridCacheEntry} given cache key.
     * If current cache is DHT and key doesn't belong to current partition,
     * {@code null} is returned.
     *
     * @param ctx Cache context.
     * @param <K> Cache key type.
     * @param <V> Cache value type.
     * @return Closure which returns {@link GridCacheEntry} given cache key or {@code null} if partition is invalid.
     */
    public static <K, V> IgniteClosure<K, GridCacheEntry<K, V>> cacheKey2Entry(
        final GridCacheContext<K, V> ctx) {
        return new IgniteClosure<K, GridCacheEntry<K, V>>() {
            @Nullable @Override public GridCacheEntry<K, V> apply(K k) {
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
    public static <K, V> IgniteClosure<GridDhtLocalPartition<K, V>, GridDhtPartitionState> part2state() {
        return PART2STATE;
    }

    /**
     * @return Not evicted partitions.
     */
    @SuppressWarnings( {"unchecked"})
    public static <K, V> GridPredicate<GridDhtLocalPartition<K, V>> notEvicted() {
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
        return allNodes(ctx, -1);
    }

    /**
     * Gets all nodes on which cache with the same name is started.
     *
     * @param ctx Cache context.
     * @param topOrder Maximum allowed node order.
     * @return All nodes on which cache with the same name is started (including nodes
     *      that may have already left).
     */
    public static Collection<ClusterNode> allNodes(GridCacheContext ctx, long topOrder) {
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
    public static Collection<ClusterNode> allNodes(GridCacheSharedContext ctx, long topOrder) {
        return ctx.discovery().cacheNodes(topOrder);
    }

    /**
     * Gets alive nodes.
     *
     * @param ctx Cache context.
     * @param topOrder Maximum allowed node order.
     * @return Affinity nodes.
     */
    public static Collection<ClusterNode> aliveNodes(final GridCacheContext ctx, long topOrder) {
        return ctx.discovery().aliveCacheNodes(ctx.namex(), topOrder);
    }

    /**
     * Gets remote nodes on which cache with the same name is started.
     *
     * @param ctx Cache context.
     * @return Remote nodes on which cache with the same name is started.
     */
    public static Collection<ClusterNode> remoteNodes(final GridCacheContext ctx) {
        return remoteNodes(ctx, -1);
    }

    /**
     * Gets remote node with at least one cache configured.
     *
     * @param ctx Shared cache context.
     * @return Collection of nodes with at least one cache configured.
     */
    public static Collection<ClusterNode> remoteNodes(GridCacheSharedContext ctx) {
        return remoteNodes(ctx, -1);
    }

    /**
     * Gets remote nodes on which cache with the same name is started.
     *
     * @param ctx Cache context.
     * @param topOrder Maximum allowed node order.
     * @return Remote nodes on which cache with the same name is started.
     */
    public static Collection<ClusterNode> remoteNodes(final GridCacheContext ctx, long topOrder) {
        return ctx.discovery().remoteCacheNodes(ctx.namex(), topOrder);
    }

    /**
     * Gets alive nodes.
     *
     * @param ctx Cache context.
     * @param topOrder Maximum allowed node order.
     * @return Affinity nodes.
     */
    public static Collection<ClusterNode> aliveRemoteNodes(final GridCacheContext ctx, long topOrder) {
        return ctx.discovery().aliveRemoteCacheNodes(ctx.namex(), topOrder);
    }

    /**
     * Gets remote nodes with at least one cache configured.
     *
     * @param ctx Cache shared context.
     * @param topVer Topology version.
     * @return Collection of remote nodes with at least one cache configured.
     */
    public static Collection<ClusterNode> remoteNodes(final GridCacheSharedContext ctx, long topVer) {
        return ctx.discovery().remoteCacheNodes(topVer);
    }

    /**
     * Gets alive nodes with at least one cache configured.
     *
     * @param ctx Cache context.
     * @param topOrder Maximum allowed node order.
     * @return Affinity nodes.
     */
    public static Collection<ClusterNode> aliveCacheNodes(final GridCacheSharedContext ctx, long topOrder) {
        return ctx.discovery().aliveNodesWithCaches(topOrder);
    }

    /**
     * Gets alive remote nodes with at least one cache configured.
     *
     * @param ctx Cache context.
     * @param topOrder Maximum allowed node order.
     * @return Affinity nodes.
     */
    public static Collection<ClusterNode> aliveRemoteCacheNodes(final GridCacheSharedContext ctx, long topOrder) {
        return ctx.discovery().aliveRemoteNodesWithCaches(topOrder);
    }

    /**
     * Gets all nodes on which cache with the same name is started and the local DHT storage is enabled.
     *
     * @param ctx Cache context.
     * @return All nodes on which cache with the same name is started.
     */
    public static Collection<ClusterNode> affinityNodes(final GridCacheContext ctx) {
        return ctx.discovery().cacheAffinityNodes(ctx.namex(), -1);
    }

    /**
     * Checks if node is affinity node for given cache configuration.
     *
     * @param cfg Configuration to check.
     * @return {@code True} if local node is affinity node (i.e. will store partitions).
     */
    public static boolean isAffinityNode(GridCacheConfiguration cfg) {
        if (cfg.getCacheMode() == LOCAL)
            return true;

        GridCacheDistributionMode partTax = cfg.getDistributionMode();

        if (partTax == null)
            partTax = distributionMode(cfg);

        return partTax == GridCacheDistributionMode.PARTITIONED_ONLY ||
            partTax == GridCacheDistributionMode.NEAR_PARTITIONED;
    }

    /**
     * Gets DHT affinity nodes.
     *
     * @param ctx Cache context.
     * @param topOrder Maximum allowed node order.
     * @return Affinity nodes.
     */
    public static Collection<ClusterNode> affinityNodes(GridCacheContext ctx, long topOrder) {
        return ctx.discovery().cacheAffinityNodes(ctx.namex(), topOrder);
    }

    /**
     * Checks if given node has specified cache started and the local DHT storage is enabled.
     *
     * @param ctx Cache context.
     * @param s Node shadow to check.
     * @return {@code True} if given node has specified cache started.
     */
    public static boolean affinityNode(GridCacheContext ctx, ClusterNode s) {
        assert ctx != null;
        assert s != null;

        GridCacheAttributes[] caches = s.attribute(ATTR_CACHE);

        if (caches != null)
            for (GridCacheAttributes attrs : caches)
                if (F.eq(ctx.namex(), attrs.cacheName()))
                    return attrs.isAffinityNode();

        return false;
    }

    /**
     * Checks if given node contains configured cache with the name
     * as described by given cache context.
     *
     * @param ctx Cache context.
     * @param node Node to check.
     * @return {@code true} if node contains required cache.
     */
    public static boolean cacheNode(GridCacheContext ctx, ClusterNode node) {
        assert ctx != null;
        assert node != null;

        return U.hasCache(node, ctx.namex());
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
    public static boolean isNearEnabled(GridCacheConfiguration cfg) {
        if (cfg.getCacheMode() == LOCAL)
            return false;

        return cfg.getDistributionMode() == NEAR_PARTITIONED ||
            cfg.getDistributionMode() == GridCacheDistributionMode.NEAR_ONLY;
    }

    /**
     * Gets default partitioned cache mode.
     *
     * @param cfg Configuration.
     * @return Partitioned cache mode.
     */
    public static GridCacheDistributionMode distributionMode(GridCacheConfiguration cfg) {
        return cfg.getDistributionMode() != null ?
            cfg.getDistributionMode() : GridCacheDistributionMode.PARTITIONED_ONLY;
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
        return oldest(cctx, -1);
    }

    /**
     * Gets oldest alive node across nodes with at least one cache configured.
     *
     * @param ctx Cache context.
     * @return Oldest node.
     */
    public static ClusterNode oldest(GridCacheSharedContext ctx) {
        return oldest(ctx, -1);
    }

    /**
     * Gets oldest alive node for specified topology version.
     *
     * @param cctx Cache context.
     * @param topOrder Maximum allowed node order.
     * @return Oldest node for the given topology version.
     */
    public static ClusterNode oldest(GridCacheContext cctx, long topOrder) {
        ClusterNode oldest = null;

        for (ClusterNode n : aliveNodes(cctx, topOrder))
            if (oldest == null || n.order() < oldest.order())
                oldest = n;

        assert oldest != null;
        assert oldest.order() <= topOrder || topOrder < 0;

        return oldest;
    }

    /**
     * Gets oldest alive node with at least one cache configured for specified topology version.
     *
     * @param cctx Shared cache context.
     * @param topOrder Maximum allowed node order.
     * @return Oldest node for the given topology version.
     */
    public static ClusterNode oldest(GridCacheSharedContext cctx, long topOrder) {
        ClusterNode oldest = null;

        for (ClusterNode n : aliveCacheNodes(cctx, topOrder))
            if (oldest == null || n.order() < oldest.order())
                oldest = n;

        assert oldest != null;
        assert oldest.order() <= topOrder || topOrder < 0;

        return oldest;
    }

    /**
     * @return Empty filter.
     */
    @SuppressWarnings({"unchecked"})
    public static <K, V> GridPredicate<GridCacheEntry<K, V>>[] empty() {
        return (GridPredicate<GridCacheEntry<K, V>>[])EMPTY_FILTER;
    }

    /**
     * @return Always false filter.
     */
    @SuppressWarnings({"unchecked"})
    public static <K, V> GridPredicate<GridCacheEntry<K, V>>[] alwaysFalse() {
        return (GridPredicate<GridCacheEntry<K, V>>[])ALWAYS_FALSE;
    }

    /**
     * @return Closure that converts tx entry to key.
     */
    @SuppressWarnings({"unchecked"})
    public static <K, V> IgniteClosure<GridCacheTxEntry<K, V>, K> tx2key() {
        return (IgniteClosure<GridCacheTxEntry<K, V>, K>)tx2key;
    }

    /**
     * @return Closure that converts tx entry collection to key collection.
     */
    @SuppressWarnings({"unchecked"})
    public static <K, V> IgniteClosure<Collection<GridCacheTxEntry<K, V>>, Collection<K>> txCol2Key() {
        return (IgniteClosure<Collection<GridCacheTxEntry<K, V>>, Collection<K>>)txCol2key;
    }

    /**
     * @return Closure that converts tx entry to key.
     */
    @SuppressWarnings({"unchecked"})
    public static <K, V> IgniteClosure<GridCacheTxEntry<K, V>, byte[]> tx2keyBytes() {
        return (IgniteClosure<GridCacheTxEntry<K, V>, byte[]>)tx2keyBytes;
    }

    /**
     * @return Converts transaction entry to cache entry.
     */
    @SuppressWarnings( {"unchecked"})
    public static <K, V> IgniteClosure<GridCacheTxEntry<K, V>, GridCacheEntryEx<K, V>> tx2entry() {
        return (IgniteClosure<GridCacheTxEntry<K, V>, GridCacheEntryEx<K, V>>)tx2entry;
    }

    /**
     * @return Closure which converts transaction entry xid to XID version.
     */
    @SuppressWarnings( {"unchecked"})
    public static <K, V> IgniteClosure<GridCacheTxEx<K, V>, GridCacheVersion> tx2xidVersion() {
        return (IgniteClosure<GridCacheTxEx<K, V>, GridCacheVersion>)tx2xidVer;
    }

    /**
     * @return Closure which converts transaction to xid.
     */
    public static IgniteClosure<GridCacheTx, GridUuid> tx2xid() {
        return tx2xid;
    }

    /**
     * @return Closure that converts entry to key.
     */
    @SuppressWarnings({"unchecked"})
    public static <K, V> IgniteClosure<GridCacheEntryEx<K, V>, K> entry2Key() {
        return (IgniteClosure<GridCacheEntryEx<K, V>, K>)entry2key;
    }

    /**
     * @return Closure that converts entry info to key.
     */
    @SuppressWarnings({"unchecked"})
    public static <K, V> IgniteClosure<GridCacheEntryInfo<K, V>, K> info2Key() {
        return (IgniteClosure<GridCacheEntryInfo<K, V>, K>)info2key;
    }

    /**
     * @return Filter for transaction reads.
     */
    @SuppressWarnings({"unchecked"})
    public static <K, V> GridPredicate<GridCacheTxEntry<K, V>> reads() {
        return READ_FILTER;
    }

    /**
     * @return Filter for transaction writes.
     */
    @SuppressWarnings({"unchecked"})
    public static <K, V> GridPredicate<GridCacheTxEntry<K, V>> writes() {
        return WRITE_FILTER;
    }

    /**
     * @return Transfer required predicate.
     */
    @SuppressWarnings("unchecked")
    public static <K, V> GridPredicate<GridCacheTxEntry<K, V>> transferRequired() {
        return TRANSFER_REQUIRED_PREDICATE;
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
     * @return Boolean reducer.
     */
    public static GridReducer<Boolean, Boolean> boolReducer() {
        return new GridReducer<Boolean, Boolean>() {
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
    public static <K, V> GridReducer<Map<K, V>, Map<K, V>> mapsReducer(final int size) {
        return new GridReducer<Map<K, V>, Map<K, V>>() {
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
    public static <T> GridReducer<Collection<T>, Collection<T>> collectionsReducer() {
        return new GridReducer<Collection<T>, Collection<T>>() {
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
    public static <T> GridReducer<T, Collection<T>> objectsReducer() {
        return new GridReducer<T, Collection<T>>() {
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
    public static GridInClosure<GridFuture<?>> errorLogger(final GridLogger log,
        final Class<? extends Exception>... excl) {
        return new CI1<GridFuture<?>>() {
            @Override public void apply(GridFuture<?> f) {
                try {
                    f.get();
                }
                catch (GridException e) {
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

        long topVer = ctx.discovery().topologyVersion();

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

        while (t instanceof GridException || t instanceof GridRuntimeException)
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

        while (t instanceof GridException || t instanceof GridRuntimeException)
            t = t.getCause();

        return t instanceof GridCacheLockTimeoutException || t instanceof GridDistributedLockCancelledException;
    }

    /**
     * @param ctx Cache context.
     * @param obj Object to marshal.
     * @return Buffer that contains obtained byte array.
     * @throws GridException If marshalling failed.
     */
    @SuppressWarnings("unchecked")
    public static byte[] marshal(GridCacheSharedContext ctx, Object obj)
        throws GridException {
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
     * @throws GridException If execution failed.
     */
    public static <T> T outTx(Callable<T> cmd, GridCacheContext ctx) throws GridException {
        if (ctx.tm().inUserTx())
            return ctx.closures().callLocalSafe(cmd, false).get();
        else {
            try {
                return cmd.call();
            }
            catch (GridException | GridRuntimeException e) {
                throw e;
            }
            catch (Exception e) {
                throw new GridException(e);
            }
        }
    }

    /**
     * @param ctx Context.
     * @param prj Projection.
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @return New transaction.
     */
    public static GridCacheTx txStartInternal(GridCacheContext ctx, GridCacheProjection prj,
        GridCacheTxConcurrency concurrency, GridCacheTxIsolation isolation) {
        assert ctx != null;
        assert prj != null;

        ctx.tm().txContextReset();

        return prj.txStart(concurrency, isolation);
    }

    /**
     * @param tx Transaction.
     * @return String view of all safe-to-print transaction properties.
     */
    public static String txString(@Nullable GridCacheTx tx) {
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
    public static void resetTxContext(GridCacheSharedContext ctx) {
        assert ctx != null;

        ctx.tm().txContextReset();
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

        GridCacheConfiguration cfg = ctx.cache().configuration();

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
     * @param rmt Remote node.
     * @param attr Attribute name.
     * @param fail If true throws GridException in case of attribute values mismatch, otherwise logs warning.
     * @throws GridException If attribute values are different and fail flag is true.
     */
    public static void checkAttributeMismatch(GridLogger log, GridCacheConfiguration locCfg,
        GridCacheConfiguration rmtCfg, ClusterNode rmt, T2<String, String> attr, boolean fail) throws GridException {
        assert rmt != null;
        assert attr != null;
        assert attr.get1() != null;
        assert attr.get2() != null;

        Object locVal = U.property(locCfg, attr.get1());

        Object rmtVal = U.property(rmtCfg, attr.get1());

        checkAttributeMismatch(log, rmtCfg.getName(), rmt, attr.get1(), attr.get2(), locVal, rmtVal, fail);
    }

    /**
     * Checks that cache configuration attribute has the same value in local and remote cache configurations.
     *
     * @param log Logger used to log warning message (used only if fail flag is not set).
     * @param cfgName Remote cache name.
     * @param rmt Remote node.
     * @param attrName Short attribute name for error message.
     * @param attrMsg Full attribute name for error message.
     * @param locVal Local value.
     * @param rmtVal Remote value.
     * @param fail If true throws GridException in case of attribute values mismatch, otherwise logs warning.
     * @throws GridException If attribute values are different and fail flag is true.
     */
    public static void checkAttributeMismatch(GridLogger log, String cfgName, ClusterNode rmt, String attrName,
        String attrMsg, @Nullable Object locVal, @Nullable Object rmtVal, boolean fail) throws GridException {
        assert rmt != null;
        assert attrName != null;
        assert attrMsg != null;

        if (!F.eq(locVal, rmtVal)) {
            if (fail) {
                throw new GridException(attrMsg + " mismatch (fix " + attrMsg.toLowerCase() + " in cache " +
                    "configuration or set -D" + GG_SKIP_CONFIGURATION_CONSISTENCY_CHECK + "=true " +
                    "system property) [cacheName=" + cfgName +
                    ", local" + capitalize(attrName) + "=" + locVal +
                    ", remote" + capitalize(attrName) + "=" + rmtVal +
                    ", rmtNodeId=" + rmt.id() + ']');
            }
            else {
                assert log != null;

                U.warn(log, attrMsg + " mismatch (fix " + attrMsg.toLowerCase() + " in cache " +
                    "configuration) [cacheName=" + cfgName +
                    ", local" + capitalize(attrName) + "=" + locVal +
                    ", remote" + capitalize(attrName) + "=" + rmtVal +
                    ", rmtNodeId=" + rmt.id() + ']');
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
    public static void validateCacheValue(GridLogger log, @Nullable Object val) {
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
    public static void validateCacheKey(GridLogger log, @Nullable Object key) {
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
    public static GridCacheConfiguration hadoopSystemCache() {
        GridCacheConfiguration cache = new GridCacheConfiguration();

        cache.setName(CU.SYS_CACHE_HADOOP_MR);
        cache.setCacheMode(REPLICATED);
        cache.setAtomicityMode(TRANSACTIONAL);
        cache.setWriteSynchronizationMode(FULL_SYNC);

        cache.setEvictionPolicy(null);
        cache.setSwapEnabled(false);
        cache.setQueryIndexEnabled(false);
        cache.setStore(null);
        cache.setEagerTtl(true);
        cache.setPreloadMode(SYNC);

        return cache;
    }

    /**
     * @param cacheName Cache name.
     * @return {@code True} if this is security system cache.
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
     * Validates that cache key or cache value implements {@link Externalizable}
     *
     * @param log Logger used to log warning message.
     * @param obj Cache key or cache value.
     */
    private static void validateExternalizable(GridLogger log, Object obj) {
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
     * @return {@code True} in this is GGFS data or meta cache.
     */
    public static boolean isGgfsCache(IgniteConfiguration cfg, @Nullable String cacheName) {
        GridGgfsConfiguration[] ggfsCfgs = cfg.getGgfsConfiguration();

        if (ggfsCfgs != null) {
            for (GridGgfsConfiguration ggfsCfg : ggfsCfgs) {
                // GGFS config probably has not been validated yet => possible NPE, so we check for null.
                if (ggfsCfg != null &&
                    (F.eq(cacheName, ggfsCfg.getDataCacheName()) || F.eq(cacheName, ggfsCfg.getMetaCacheName())))
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
        assert ttl >= 0L;

        if (ttl == 0L)
            return 0L;
        else {
            long expireTime = U.currentTimeMillis() + ttl;

            return expireTime > 0L ? expireTime : 0L;
        }
    }

    /**
     * Execute closure inside cache transaction.
     *
     * @param cache Cache.
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @param clo Closure.
     * @throws GridException If failed.
     */
    public static <K, V> void inTx(GridCacheProjection<K, V> cache, GridCacheTxConcurrency concurrency,
        GridCacheTxIsolation isolation, GridInClosureX<GridCacheProjection<K ,V>> clo) throws GridException {

        try (GridCacheTx tx = cache.txStart(concurrency, isolation)) {
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
    public static <K, V> UUID subjectId(GridCacheTxEx<K, V> tx, GridCacheSharedContext<K, V> ctx) {
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
    public static <K, V> boolean invalidate(GridCacheProjection<K, V> cache, K key) {
        return cache.clear(key);
    }
}
