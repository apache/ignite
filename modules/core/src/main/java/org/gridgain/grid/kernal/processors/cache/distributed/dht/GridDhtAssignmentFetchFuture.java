/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.dht;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.preloader.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

import static org.gridgain.grid.kernal.managers.communication.GridIoPolicy.*;

/**
 * Future that fetches affinity assignment from remote cache nodes.
 */
public class GridDhtAssignmentFetchFuture<K, V> extends GridFutureAdapter<List<List<GridNode>>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Nodes order comparator. */
    private static final Comparator<GridNode> CMP = new GridNodeOrderComparator();

    /** Cache context. */
    private final GridCacheContext<K, V> ctx;

    /** List of available nodes this future can fetch data from. */
    private Queue<GridNode> availableNodes;

    /** Topology version. */
    private final long topVer;

    /** Pending node from which response is being awaited. */
    private GridNode pendingNode;

    /**
     * @param ctx Cache context.
     * @param availableNodes Available nodes.
     */
    public GridDhtAssignmentFetchFuture(GridCacheContext<K, V> ctx, long topVer, Collection<GridNode> availableNodes) {
        super(ctx.kernalContext());

        this.ctx = ctx;

        this.topVer = topVer;

        LinkedList<GridNode> tmp = new LinkedList<>();
        tmp.addAll(availableNodes);
        Collections.sort(tmp, CMP);

        this.availableNodes = tmp;
    }

    /**
     * Initializes fetch future.
     */
    public void init() {
        ((GridDhtPreloader<K, V>)ctx.preloader()).addDhtAssignmentFetchFuture(topVer, this);

        requestFromNextNode();
    }

    /**
     * @param node Node.
     * @param res Reponse.
     */
    public void onResponse(GridNode node, GridDhtAffinityAssignmentResponse<K, V> res) {
        if (res.topologyVersion() != topVer) {
            if (log.isDebugEnabled())
                log.debug("Received affinity assignment for wrong topolgy version (will ignore) " +
                    "[node=" + node + ", res=" + res + ", topVer=" + topVer + ']');

            return;
        }

        List<List<GridNode>> assignment = null;

        synchronized (this) {
            if (pendingNode != null && pendingNode.equals(node))
                assignment = res.affinityAssignment();
        }

        if (assignment != null)
            onDone(assignment);
    }

    /**
     * @param leftNodeId Left node ID.
     */
    public void onNodeLeft(UUID leftNodeId) {
        synchronized (this) {
            if (pendingNode != null && pendingNode.id().equals(leftNodeId)) {
                availableNodes.remove(pendingNode);

                pendingNode = null;
            }
        }

        requestFromNextNode();
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(@Nullable List<List<GridNode>> res, @Nullable Throwable err) {
        if (super.onDone(res, err)) {
            ((GridDhtPreloader<K, V>)ctx.preloader()).removeDhtAssignmentFetchFuture(topVer, this);

            return true;
        }

        return false;
    }

    /**
     * Requests affinity from next node in the list.
     */
    private void requestFromNextNode() {
        boolean complete;

        // Avoid 'protected field is accessed in synchronized context' warning.
        GridLogger log0 = log;

        synchronized (this) {
            while (!availableNodes.isEmpty()) {
                GridNode node = availableNodes.poll();

                try {
                    if (log0.isDebugEnabled())
                        log0.debug("Sending affinity fetch request to remote node [locNodeId=" + ctx.localNodeId() +
                            ", node=" + node + ']');

                    ctx.io().send(node, new GridDhtAffinityAssignmentRequest<K, V>(ctx.cacheId(), topVer),
                        AFFINITY_POOL);

                    // Close window for listener notification.
                    if (ctx.discovery().node(node.id()) == null) {
                        U.warn(log0, "Failed to request affinity assignment from remote node (node left grid, will " +
                            "continue to another node): " + node);

                        continue;
                    }

                    pendingNode = node;

                    break;
                }
                catch (GridTopologyException ignored) {
                    U.warn(log0, "Failed to request affinity assignment from remote node (node left grid, will " +
                        "continue to another node): " + node);
                }
                catch (GridException e) {
                    U.error(log0, "Failed to request affinity assignment from remote node (will " +
                        "continue to another node): " + node, e);
                }
            }

            complete = pendingNode == null;
        }

        // No more nodes left, complete future with null outside of synchronization.
        // Affinity should be calculated from scratch.
        if (complete)
            onDone((List<List<GridNode>>)null);
    }
}
