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

import java.util.*;
import java.util.concurrent.*;

/**
 * Cache set iterator.
 */
class GridCacheSetIterator<T> implements Iterator<T> {
    /** Set. */
    private GridCacheSet set;

    /** Nodes data request should be sent to. */
    private Collection<UUID> nodes;

    /** Responses queue. */
    private LinkedBlockingQueue<GridCacheSetDataResponse> resQueue = new LinkedBlockingQueue<>();

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

    /**
     * @param set Cache set.
     * @param nodes Nodes data request should be sent to.
     * @param req Set data request.
     * @throws org.gridgain.grid.GridException If failed.
     */
    GridCacheSetIterator(GridCacheSet set, Collection<UUID> nodes, GridCacheSetDataRequest req)
        throws GridException {
        this.set = set;
        this.nodes = nodes;
        this.req = req;

        sendIteratorRequest(req, nodes);
    }

    /**
     * @param res Data response.
     */
    void onResponse(GridCacheSetDataResponse res) {
        boolean add = resQueue.add(res);

        assert add;
    }

    /** {@inheritDoc} */
    @Override public T next() {
        init();

        if (next == null)
            throw new NoSuchElementException();

        cur = next;
        next = next0();

        return (T)cur;
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
                if (nodes.isEmpty())
                    return null;

                GridCacheSetDataResponse res = resQueue.take();

                if (res.last())
                    nodes.remove(res.nodeId());
                else
                    sendIteratorRequest(req, Collections.singleton(res.nodeId()));

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
    private void sendIteratorRequest(final GridCacheSetDataRequest req, Collection<UUID> nodeIds)
        throws GridException {
        assert !nodeIds.isEmpty();

        // TODO: handle send errors.

        for (UUID nodeId : nodeIds) {
            if (nodeId.equals(set.context().localNodeId())) {
                set.context().closures().callLocalSafe(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        ((GridCacheEnterpriseDataStructuresManager)set.context().dataStructures()).
                            processSetIteratorRequest(set.context().localNodeId(), req);

                        return null;
                    }
                });
            }
            else
                set.context().io().send(nodeId, (GridCacheMessage)req.clone());
        }
    }
}
