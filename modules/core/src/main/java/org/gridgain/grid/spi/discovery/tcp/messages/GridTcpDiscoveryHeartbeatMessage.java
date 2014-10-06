/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.discovery.tcp.messages;

import org.gridgain.grid.*;
import org.gridgain.grid.spi.discovery.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;

import java.io.*;
import java.util.*;

/**
 * Heartbeat message.
 * <p>
 * It is sent by coordinator node across the ring once a configured period.
 * In case if topology does not use metrics store, message makes two passes.
 * <ol>
 *      <li>During first pass, all nodes add their metrics to the message and
 *          update local metrics with metrics currently present in the message.</li>
 *      <li>During second pass, all nodes update all metrics present in the message
 *          and remove their own metrics from the message.</li>
 * </ol>
 * When message reaches coordinator second time it is discarded (it finishes the
 * second pass).
 * <p>
 * If topology uses metrics store then message makes only one pass and metrics map
 * is always empty. Nodes exchange their metrics using metrics store.
 */
public class GridTcpDiscoveryHeartbeatMessage extends GridTcpDiscoveryAbstractMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Map to store nodes metrics. */
    @GridToStringExclude
    private Map<UUID, byte[]> metrics;

    /** Client node IDs. */
    private Collection<UUID> clientNodeIds;

    /**
     * Public default no-arg constructor for {@link Externalizable} interface.
     */
    public GridTcpDiscoveryHeartbeatMessage() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param creatorNodeId Creator node.
     */
    public GridTcpDiscoveryHeartbeatMessage(UUID creatorNodeId) {
        super(creatorNodeId);

        metrics = new HashMap<>(1, 1.0f);
        clientNodeIds = new HashSet<>();
    }

    /**
     * Sets metrics for particular node.
     *
     * @param nodeId Node ID.
     * @param metrics Node metrics.
     */
    public void setMetrics(UUID nodeId, GridNodeMetrics metrics) {
        assert nodeId != null;
        assert metrics != null;

        byte[] buf = new byte[GridDiscoveryMetricsHelper.METRICS_SIZE];

        GridDiscoveryMetricsHelper.serialize(buf, 0, metrics);

        this.metrics.put(nodeId, buf);
    }

    /**
     * @param clientNodeIds Client node IDs.
     */
    public void addClientNodeIds(Collection<UUID> clientNodeIds) {
        assert clientNodeIds != null;

        this.clientNodeIds.addAll(clientNodeIds);
    }

    /**
     * Removes metrics for particular node from the message.
     *
     * @param nodeId Node ID.
     */
    public void removeMetrics(UUID nodeId) {
        assert nodeId != null;

        metrics.remove(nodeId);
    }

    /**
     * Gets metrics map.
     *
     * @return Metrics map.
     */
    public Map<UUID, GridNodeMetrics> metrics() {
        return F.viewReadOnly(metrics, new C1<byte[], GridNodeMetrics>() {
            @Override public GridNodeMetrics apply(byte[] metricsBytes) {
                return GridDiscoveryMetricsHelper.deserialize(metricsBytes, 0);
            }
        });
    }

    /**
     * @return {@code True} if this message contains metrics.
     */
    public boolean hasMetrics() {
        return !metrics.isEmpty();
    }

    /**
     * @return {@code True} if this message contains metrics.
     */
    public boolean hasMetrics(UUID nodeId) {
        assert nodeId != null;

        return metrics.get(nodeId) != null;
    }

    /**
     * Gets client node IDs for  particular node.
     *
     * @return Client node IDs.
     */
    public Collection<UUID> clientNodeIds() {
        return clientNodeIds;
    }

    /**
     * Clears client node IDs collection.
     */
    public void clearClientNodeIds() {
        clientNodeIds.clear();
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        out.writeInt(metrics.size());

        if (!metrics.isEmpty()) {
            for (Map.Entry<UUID, byte[]> e : metrics.entrySet()) {
                U.writeUuid(out, e.getKey());

                U.writeByteArray(out, e.getValue());
            }
        }

        U.writeCollection(out, clientNodeIds);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        int metricsSize = in.readInt();

        metrics = new HashMap<>(metricsSize + 1, 1.0f);

        for (int i = 0; i < metricsSize; i++)
            metrics.put(U.readUuid(in), U.readByteArray(in));

        clientNodeIds = U.readCollection(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridTcpDiscoveryHeartbeatMessage.class, this, "super", super.toString());
    }
}
