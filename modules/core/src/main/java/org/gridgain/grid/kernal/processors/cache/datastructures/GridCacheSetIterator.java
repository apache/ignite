/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.datastructures;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.logger.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * Cache set iterator.
 */
class GridCacheSetIterator<T> implements Iterator<T> {
    /** */
    private static final Object FAILED = new Object();

    /** Set. */
    private GridCacheSet set;

    /** Logger. */
    private GridLogger log;

    /** Nodes data request should be sent to. */
    private Collection<UUID> nodeIds;

    /** Responses queue. */
    private LinkedBlockingQueue<Object> resQueue = new LinkedBlockingQueue<>();

    /** Current iterator. */
    private Iterator<T> it;

    /** Next item. */
    private T next;

    /** Current item. */
    private T cur;

    /** Set data request. */
    private GridCacheSetDataRequest req;

    /** Initialization flag. */
    private boolean init;

    /** */
    private GridException err;

    /**
     * @param set Cache set.
     * @param nodeIds Node IDs data request should be sent to.
     * @param req Set data request.
     */
    @SuppressWarnings("unchecked")
    GridCacheSetIterator(GridCacheSet set, Collection<UUID> nodeIds, GridCacheSetDataRequest req) {
        this.set = set;
        this.nodeIds = nodeIds;
        this.req = req;

        log = set.context().logger(GridCacheSetIterator.class);
    }

    /**
     * @throws GridException If failed.
     */
    void sendFirstRequest() throws GridException {
        sendRequest(req, nodeIds);
    }

    /**
     * @param res Data response.
     */
    void onResponse(GridCacheSetDataResponse res) {
        boolean add = resQueue.add(res);

        assert add;
    }

    /**
     * @param nodeId Node ID.
     * @return {@code True} if coresponding response handler should be removed.
     */
    boolean onNodeLeft(UUID nodeId) {
        if (nodeIds.remove(nodeId)) {
            err = new GridException("Failed to get set data, node left grid: " + nodeId);

            resQueue.add(FAILED);

            return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public T next() {
        init();

        if (next == null)
            throw new NoSuchElementException();

        cur = next;
        next = next0();

        return cur;
    }

    /** {@inheritDoc} */
    @Override public boolean hasNext() {
        init();

        return next != null;
    }

    /**
     * Initializes iterator.
     */
    private void init() {
        if (!init) {
            next = next0();

            init = true;
        }
    }

    /** {@inheritDoc} */
    @Override public void remove() {
        if (cur == null)
            throw new NoSuchElementException();

        set.remove(cur);

        cur = null;
    }

    /**
     * @return Next element.
     */
    @SuppressWarnings("unchecked")
    public T next0() {
        try {
            while (it == null || !it.hasNext()) {
                if (nodeIds.isEmpty()) {
                    req.id();

                    return null;
                }

                GridCacheSetDataResponse res = null;

                while (res == null) {
                    Object poll = resQueue.poll(1000, TimeUnit.MILLISECONDS);

                    if (log.isDebugEnabled())
                        log.debug("Polled set data response " + res);

                    if (poll == null)
                        continue;

                    if (poll == FAILED) {
                        assert err != null;

                        throw err;
                    }
                    else
                        res = (GridCacheSetDataResponse)poll;
                }

                if (res.last()) {
                    nodeIds.remove(res.nodeId());

                    if (log.isDebugEnabled())
                        log.debug("Received last set data response [nodeId=" + res.nodeId() +
                            ", nodeIds=" + nodeIds + ']');

                    if (nodeIds.isEmpty()) {
                        GridCacheEnterpriseDataStructuresManager ds =
                            (GridCacheEnterpriseDataStructuresManager)set.context().dataStructures();

                        ds.removeSetResponseHandler(req.id());
                    }
                }
                else {
                    if (log.isDebugEnabled())
                        log.debug("Received data response [nodeId=" + res.nodeId() + ']');

                    sendRequest(req, Collections.singleton(res.nodeId()));
                }

                it = res.data().iterator();
            }

            return it.next();
        }
        catch (Exception e) {
            throw new GridRuntimeException(e);
        }
    }

    /**
     * @param req Request.
     * @param nodeIds Node IDs.
     * @throws GridException If failed.
     */
    @SuppressWarnings("unchecked")
    private void sendRequest(final GridCacheSetDataRequest req, Collection<UUID> nodeIds)
        throws GridException {
        assert !nodeIds.isEmpty();

        final GridCacheEnterpriseDataStructuresManager ds =
            ((GridCacheEnterpriseDataStructuresManager)set.context().dataStructures());

        for (UUID nodeId : nodeIds) {
            if (nodeId.equals(set.context().localNodeId())) {
                set.context().closures().callLocalSafe(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        ds.processSetDataRequest(set.context().localNodeId(), req);

                        return null;
                    }
                });
            }
            else {
                try {
                    set.context().io().send(nodeId, (GridCacheMessage)req.clone());
                }
                catch (GridException e) {
                    if (req.cancel()) {
                        if (log.isDebugEnabled())
                            log.debug("Failed to cancel set data request [nodeId=" + nodeId + ", err=" + e + ']');
                    }
                    else {
                        cancel();

                        throw e;
                    }
                }
            }
        }
    }

    /**
     * Sends cancel requests.
     * @throws GridException If failed.
     */
    private void cancel() throws GridException {
        GridCacheEnterpriseDataStructuresManager ds =
            ((GridCacheEnterpriseDataStructuresManager)set.context().dataStructures());

        ds.removeSetResponseHandler(req.id());

        GridCacheSetDataRequest cancelReq = new GridCacheSetDataRequest(req.id(), set.id(), 0, 0, false);

        cancelReq.cancel(true);

        sendRequest(cancelReq, nodeIds);
    }
}
