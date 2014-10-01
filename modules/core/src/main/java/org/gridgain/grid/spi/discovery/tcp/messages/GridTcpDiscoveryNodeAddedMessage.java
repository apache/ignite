/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.discovery.tcp.messages;

import org.gridgain.grid.*;
import org.gridgain.grid.spi.discovery.tcp.internal.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Message telling nodes that new node should be added to topology.
 * When newly added node receives the message it connects to its next and finishes
 * join process.
 */
@GridTcpDiscoveryEnsureDelivery
public class GridTcpDiscoveryNodeAddedMessage extends GridTcpDiscoveryAbstractMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Added node. */
    private GridTcpDiscoveryNode node;

    /** Pending messages from previous node. */
    private Collection<GridTcpDiscoveryAbstractMessage> msgs;

    /** Current topology. Initialized by coordinator. */
    @GridToStringInclude
    private Collection<GridTcpDiscoveryNode> top;

    /** Topology snapshots history. */
    private Map<Long, Collection<GridNode>> topHist;

    /** If {@code true} messages will be processed, otherwise registered. */
    @Deprecated
    private boolean procPendingMsgs; // Not used any more.

    /** Discovery data from new node. */
    private List<Object> newNodeDiscoData;

    /** Discovery data from old nodes. */
    private Collection<List<Object>> oldNodesDiscoData;

    /** Start time of the first grid node. */
    private long gridStartTime;

    /** Client node flag. */
    private boolean client;

    /**
     * Public default no-arg constructor for {@link Externalizable} interface.
     */
    public GridTcpDiscoveryNodeAddedMessage() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param creatorNodeId Creator node ID.
     * @param node Node to add to topology.
     * @param newNodeDiscoData New Node discovery data.
     * @param gridStartTime Start time of the first grid node.
     * @param client Client node flag.
     */
    public GridTcpDiscoveryNodeAddedMessage(UUID creatorNodeId, GridTcpDiscoveryNode node,
        List<Object> newNodeDiscoData, long gridStartTime, boolean client) {
        super(creatorNodeId);

        assert node != null;
        assert gridStartTime > 0;

        this.node = node;
        this.newNodeDiscoData = newNodeDiscoData;
        this.gridStartTime = gridStartTime;
        this.client = client;

        oldNodesDiscoData = new LinkedList<>();
    }

    /**
     * Gets newly added node.
     *
     * @return New node.
     */
    public GridTcpDiscoveryNode node() {
        return node;
    }

    /**
     * Gets pending messages sent to new node by its previous.
     *
     * @return Pending messages from previous node.
     */
    @Nullable public Collection<GridTcpDiscoveryAbstractMessage> messages() {
        return msgs;
    }

    /**
     * Sets pending messages to send to new node.
     *
     * @param msgs Pending messages to send to new node.
     */
    public void messages(@Nullable Collection<GridTcpDiscoveryAbstractMessage> msgs) {
        this.msgs = msgs;
    }

    /**
     * Gets topology.
     *
     * @return Current topology.
     */
    @Nullable public Collection<GridTcpDiscoveryNode> topology() {
        return top;
    }

    /**
     * Sets topology.
     *
     * @param top Current topology.
     */
    public void topology(@Nullable Collection<GridTcpDiscoveryNode> top) {
        this.top = top;
    }

    /**
     * Gets topology snapshots history.
     *
     * @return Map with topology snapshots history.
     */
    @Nullable public Map<Long, Collection<GridNode>> topologyHistory() {
        return topHist;
    }

    /**
     * Sets topology snapshots history.
     *
     * @param topHist Map with topology snapshots history.
     */
    public void topologyHistory(@Nullable Map<Long, Collection<GridNode>> topHist) {
        this.topHist = topHist;
    }

    /**
     * @return Discovery data from new node.
     */
    public List<Object> newNodeDiscoveryData() {
        return newNodeDiscoData;
    }

    /**
     * @return Discovery data from old nodes.
     */
    public Collection<List<Object>> oldNodesDiscoveryData() {
        return oldNodesDiscoData;
    }

    /**
     * @return Client node flag.
     */
    public boolean client() {
        return client;
    }

    /**
     * @param discoData Discovery data to add.
     */
    public void addDiscoveryData(List<Object> discoData) {
        // Old nodes disco data may be null if message
        // makes more than 1 pass due to stopping of the nodes in topology.
        if (oldNodesDiscoData != null)
            oldNodesDiscoData.add(discoData);
    }

    /**
     * Clears discovery data to minimize message size.
     */
    public void clearDiscoveryData() {
        newNodeDiscoData = null;
        oldNodesDiscoData = null;
    }

    /**
     * @return First grid node start time.
     */
    public long gridStartTime() {
        return gridStartTime;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        out.writeObject(node);
        U.writeCollection(out, msgs);
        U.writeCollection(out, top);
        U.writeMap(out, topHist);
        out.writeBoolean(procPendingMsgs);
        out.writeObject(newNodeDiscoData);
        U.writeCollection(out, oldNodesDiscoData);
        out.writeLong(gridStartTime);
        out.writeBoolean(client);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        node = (GridTcpDiscoveryNode)in.readObject();
        msgs = U.readCollection(in);
        top = U.readCollection(in);
        topHist = U.readTreeMap(in);
        procPendingMsgs = in.readBoolean();
        newNodeDiscoData = (List<Object>)in.readObject();
        oldNodesDiscoData = U.readCollection(in);
        gridStartTime = in.readLong();
        client = in.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridTcpDiscoveryNodeAddedMessage.class, this, "super", super.toString());
    }
}
