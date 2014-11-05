/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.managers.communication.*;
import org.gridgain.grid.kernal.managers.deployment.*;
import org.gridgain.grid.kernal.managers.discovery.*;
import org.gridgain.grid.kernal.managers.eventstorage.*;
import org.gridgain.grid.kernal.processors.timeout.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.marshaller.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.typedef.*;
import org.jetbrains.annotations.*;

import java.util.*;

import static org.gridgain.grid.cache.GridCacheFlag.*;

/**
 * Shared context.
 */
public class GridCacheSharedContext<K, V> {
    /** Cache transaction manager. */
    private GridCacheTxManager<K, V> txMgr;

    /** Partition exchange manager. */
    private GridCachePartitionExchangeManager<K, V> exchMgr;

    /** Version manager. */
    private GridCacheVersionManager<K, V> verMgr;

    /** Lock manager. */
    private GridCacheMvccManager<K, V> mvccMgr;

    /** IO Manager. */
    private GridCacheIoManager<K, V> ioMgr;

    /** Deployment manager. */
    private GridCacheDeploymentManager<K, V> deployMgr;

    /** Cache contexts map. */
    private Map<Integer, GridCacheContext<K, V>> ctxMap;

    /** Managers. */
    private List<GridCacheSharedManager<K, V>> mgrs = new LinkedList<>();

    /** Kernal context. */
    private GridKernalContext kernalContext;

    /** Tx metrics. */
    private GridCacheTxMetricsAdapter txMetrics;

    /** Preloaders start future. */
    private GridFuture<Void> preloadersStartFut;

    /**
     * @param txMgr Transaction manager.
     * @param verMgr Version manager.
     * @param mvccMgr MVCC manager.
     */
    public GridCacheSharedContext(
        GridCacheTxManager<K, V> txMgr,
        GridCacheVersionManager<K, V> verMgr,
        GridCacheMvccManager<K, V> mvccMgr
    ) {
        this.mvccMgr = add(mvccMgr);
        this.verMgr = add(verMgr);
        this.txMgr = add(txMgr);
    }

    /**
     * Gets all cache contexts for local node.
     *
     * @return Collection of all cache contexts.
     */
    public Collection<GridCacheContext<K, V>> cacheContexts() {
        return ctxMap.values();
    }

    /**
     * Gets cache context by cache ID.
     *
     * @param cacheId Cache ID.
     * @return Cache context.
     */
    public GridCacheContext<K, V> cacheContext(int cacheId) {
        return ctxMap.get(cacheId);
    }

    /**
     * @return Grid name.
     */
    public String gridName() {
        return kernalContext.gridName();
    }

    /**
     * Gets transactions configuration.
     *
     * @return Transactions configuration.
     */
    public GridTransactionsConfiguration txConfig() {
        return kernalContext.config().getTransactionsConfiguration();
    }

    /**
     * @return Timeout for initial map exchange before preloading. We make it {@code 4} times
     * bigger than network timeout by default.
     */
    public long preloadExchangeTimeout() {
        long t1 = gridConfig().getNetworkTimeout() * 4;
        long t2 = gridConfig().getNetworkTimeout() * gridConfig().getCacheConfiguration().length * 2;

        long timeout = Math.max(t1, t2);

        return timeout < 0 ? Long.MAX_VALUE : timeout;
    }

    /**
     * @return Deployment enabled flag.
     */
    public boolean deploymentEnabled() {
        return gridConfig().isPeerClassLoadingEnabled();
    }

    /**
     * @return Compound preloaders start future.
     */
    public GridFuture<Void> preloadersStartFuture() {
        if (preloadersStartFut == null) {
            GridCompoundFuture<Void, Void> compound = null;

            for (GridCacheContext<K, V> cacheCtx : cacheContexts()) {
                GridFuture<Void> startFut = cacheCtx.preloader().startFuture();

                if (!startFut.isDone()) {
                    if (compound == null)
                        compound = new GridCompoundFuture<>();

                    compound.add(startFut);
                }
            }

            if (compound != null) {
                compound.markInitialized();

                return preloadersStartFut = compound;
            }
            else
                return preloadersStartFut = new GridFinishedFuture<>();
        }
        else
            return preloadersStartFut;
    }

    /**
     * @return Transactional metrics adapter.
     */
    public GridCacheTxMetricsAdapter txMetrics() {
        return txMetrics;
    }

    /**
     * @return Cache transaction manager.
     */
    public GridCacheTxManager<K, V> tm() {
        return txMgr;
    }

    /**
     * @return Exchange manager.
     */
    public GridCachePartitionExchangeManager<K, V> exchange() {
        return exchMgr;
    }

    /**
     * @return Lock order manager.
     */
    public GridCacheVersionManager<K, V> versions() {
        return verMgr;
    }

    /**
     * @return Lock manager.
     */
    public GridCacheMvccManager<K, V> mvcc() {
        return mvccMgr;
    }

    /**
     * @return IO manager.
     */
    public GridCacheIoManager<K, V> io() {
        return ioMgr;
    }

    /**
     * @return Cache deployment manager.
     */
    public GridCacheDeploymentManager<K, V> deploy() {
        return deployMgr;
    }

    /**
     * @return Marshaller.
     */
    public GridMarshaller marshaller() {
        return kernalContext.config().getMarshaller();
    }

    /**
     * @return Grid configuration.
     */
    public GridConfiguration gridConfig() {
        return kernalContext.config();
    }

    /**
     * @return Kernal context.
     */
    public GridKernalContext kernalContext() {
        return kernalContext;
    }

    /**
     * @return Grid IO manager.
     */
    public GridIoManager gridIO() {
        return kernalContext.io();
    }

    /**
     * @return Grid deployment manager.
     */
    public GridDeploymentManager gridDeploy() {
        return kernalContext.deploy();
    }

    /**
     * @return Grid event storage manager.
     */
    public GridEventStorageManager gridEvents() {
        return kernalContext.event();
    }

    /**
     * @return Discovery manager.
     */
    public GridDiscoveryManager discovery() {
        return kernalContext.discovery();
    }

    /**
     * @return Timeout processor.
     */
    public GridTimeoutProcessor time() {
        return kernalContext.timeout();
    }

    /**
     * @return Node ID.
     */
    public UUID localNodeId() {
        return kernalContext.localNodeId();
    }

    /**
     * @return Local node.
     */
    public GridNode localNode() {
        return kernalContext.discovery().localNode();
    }

    /**
     * @param nodeId Node ID.
     * @return Node or {@code null}.
     */
    public GridNode node(UUID nodeId) {
        return kernalContext.discovery().node(nodeId);
    }

    /**
     * Gets grid logger for given class.
     *
     * @param cls Class to get logger for.
     * @return GridLogger instance.
     */
    public GridLogger logger(Class<?> cls) {
        return kernalContext.log(cls);
    }

    /**
     * @param category Category.
     * @return Logger.
     */
    public GridLogger logger(String category) {
        return kernalContext.log().getLogger(category);
    }

    /**
     * Waits for partition locks and transactions release.
     *
     * @param topVer Topology version.
     * @return {@code true} if waiting was successful.
     */
    @SuppressWarnings({"unchecked"})
    public GridFuture<?> partitionReleaseFuture(long topVer) {
        GridCompoundFuture f = new GridCompoundFuture(kernalContext);

        f.add(mvcc().finishExplicitLocks(topVer));
        f.add(tm().finishTxs(topVer));
        f.add(mvcc().finishAtomicUpdates(topVer));

        f.markInitialized();

        return f;
    }



    /**
     * @param flags Flags to turn on.
     * @throws GridCacheFlagException If given flags are conflicting with given transaction.
     */
    public void checkTxFlags(@Nullable Collection<GridCacheFlag> flags) throws GridCacheFlagException {
        GridCacheTxEx tx = tm().userTxx();

        if (tx == null || F.isEmpty(flags))
            return;

        assert flags != null;

        if (flags.contains(INVALIDATE) && !tx.isInvalidate())
            throw new GridCacheFlagException(INVALIDATE);

        if (flags.contains(SYNC_COMMIT) && !tx.syncCommit())
            throw new GridCacheFlagException(SYNC_COMMIT);
    }

    /**
     * @param mgr Manager to add.
     * @return Added manager.
     */
    @Nullable private <T extends GridCacheSharedManager<K, V>> T add(@Nullable T mgr) {
        if (mgr != null)
            mgrs.add(mgr);

        return mgr;
    }

    /**
     * Nulling references to potentially leak-prone objects.
     */
    public void cleanup() {
        mvccMgr = null;

        mgrs.clear();
    }
}
