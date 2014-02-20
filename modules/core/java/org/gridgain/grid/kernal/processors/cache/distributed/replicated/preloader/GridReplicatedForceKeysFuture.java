// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.replicated.preloader;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.future.*;
import org.jetbrains.annotations.*;

import java.util.*;

import static org.gridgain.grid.events.GridEventType.*;

import static org.gridgain.grid.kernal.processors.dr.GridDrType.*;

/**
 * Force key request future.
 *
 * @author @java.author
 * @version @java.version
 */
@SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
public class GridReplicatedForceKeysFuture<K, V> extends GridFutureAdapter<Object> {
    /** Context. */
    private final GridCacheContext<K, V> cctx;

    /** Future ID. */
    private final GridUuid futId = GridUuid.randomUuid();

    /** Preloader. */
    private final GridReplicatedPreloader<K, V> preloader;

    /** Topology version for which keys are requested. */
    private final long topVer;

    /** All remote nodes with order less then local node order. */
    private Collection<GridNode> rmtNodes;

    /** Keys to request. */
    private Collection<K> reqKeys;

    /** Node ID where request was sent. */
    private UUID reqNodeId;

    /**
     * @param cctx Context.
     * @param preloader Preloader.
     * @param topVer Topology version for which keys are requested.
     */
    public GridReplicatedForceKeysFuture(GridCacheContext<K, V> cctx, GridReplicatedPreloader<K, V> preloader,
        long topVer) {
        super(cctx.kernalContext(), true);

        this.cctx = cctx;
        this.preloader = preloader;
        this.topVer = topVer;
    }

    /**
     * @param keys Keys.
     */
    public void init(Collection<? extends K> keys) {
        final long locOrder = cctx.localNode().order();

        Collection<GridNode> elderNodes = cctx.affinity().nodes(keys);

        // Need to allow modification.
        rmtNodes = new ArrayList<>(elderNodes.size());

        for (GridNode node : elderNodes) {
            if (node.order() < locOrder)
                rmtNodes.add(node);
        }

        if (rmtNodes.isEmpty()) {
            onDone();

            return;
        }

        for (K key : keys) {
            GridCacheEntryEx<K, V> e = cctx.cache().peekEx(key);

            try {
                if (e != null && !e.isNewLocked()) {
                    if (log.isDebugEnabled())
                        log.debug("Will not preload key [cacheName=" + cctx.name() + ", key=" + key +
                            ", locId=" + cctx.nodeId() + ']');

                    continue;
                }
            }
            catch (GridCacheEntryRemovedException ignore) {
                if (log.isDebugEnabled())
                    log.debug("Received removed entry for force keys request [entry=" + e +
                        ", locId=" + cctx.nodeId() + ']');
            }

            if (reqKeys == null)
                reqKeys = new ArrayList<>(keys.size());

            reqKeys.add(key);
        }

        if (reqKeys == null) {
            onDone();

            return;
        }

        preloader.addFuture(this);

        sendRequest();
    }

    /**
     * @param res Response.
     */
    void onResult(GridReplicatedForceKeysResponse<K, V> res) {
        boolean rec = cctx.events().isRecordable(EVT_CACHE_PRELOAD_OBJECT_LOADED);

        for (GridCacheEntryInfo<K, V> info : res.forcedInfos()) {
            GridCacheEntryEx<K, V> entry = cctx.cache().entryEx(info.key());

            try {
                if (entry.initialValue(
                    info.value(),
                    info.valueBytes(),
                    info.version(),
                    info.ttl(),
                    info.expireTime(),
                    true,
                    DR_NONE
                )) {
                    if (rec && !entry.isInternal())
                        cctx.events().addEvent(entry.partition(), entry.key(), cctx.localNodeId(),
                            (GridUuid)null, null, EVT_CACHE_PRELOAD_OBJECT_LOADED, info.value(), true, null, false);
                }
            }
            catch (GridException e) {
                onDone(e);

                return;
            }
            catch (GridCacheEntryRemovedException ignore) {
                if (log.isDebugEnabled())
                    log.debug("Trying to preload removed entry (will ignore) [cacheName=" + cctx.namex() +
                        ", entry=" + entry + ']');
            }
        }

        onDone();
    }

    /**
     * @param nodeId Node ID.
     */
    @SuppressWarnings("NonPrivateFieldAccessedInSynchronizedContext")
    synchronized void onNodeLeft(UUID nodeId) {
        if (nodeId.equals(reqNodeId)) {
            if (log.isDebugEnabled())
                log.debug("Remote node left grid while waiting for reply (will retry): " + this);

            sendRequest();
        }
    }

    /**
     * Sends force key request.
     */
    @SuppressWarnings("NonPrivateFieldAccessedInSynchronizedContext")
    private synchronized void sendRequest() {
        if (isDone())
            return;

        GridNode oldest = null;

        for (GridNode n : rmtNodes) {
            GridNode node = cctx.discovery().node(n.id());

            if (node == null)
                continue;

            if (oldest == null || n.order() < oldest.order())
                oldest = n;
        }

        if (oldest == null) // All older nodes left.
            onDone(null);

        GridReplicatedForceKeysRequest<K, V> req = new GridReplicatedForceKeysRequest<>(futId, reqKeys, topVer);

        try {
            reqNodeId = oldest.id();

            cctx.io().send(oldest.id(), req);
        }
        catch (GridTopologyException ignored) {
            if (log.isDebugEnabled())
                log.debug("Remote node left grid while sending (will retry): " + this);

            rmtNodes.remove(oldest);

            sendRequest();
        }
        catch (GridException e) {
            onDone(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(@Nullable Object res, @Nullable Throwable err) {
        if (super.onDone(res, err)) {
            preloader.removeFuture(this);

            return true;
        }

        return false;
    }

    /**
     * @return Future ID.
     */
    public GridUuid futureId() {
        return futId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridReplicatedForceKeysFuture.class, this, super.toString());
    }
}
