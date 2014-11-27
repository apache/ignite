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
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

import static org.gridgain.grid.spi.discovery.GridDiscoveryMetricsHelper.*;

/**
 * Heartbeat message.
 * <p>
 * It is sent by coordinator node across the ring once a configured period.
 * Message makes two passes:
 * <ol>
 *      <li>During first pass, all nodes add their metrics to the message and
 *          update local metrics with metrics currently present in the message.</li>
 *      <li>During second pass, all nodes update all metrics present in the message
 *          and remove their own metrics from the message.</li>
 * </ol>
 * When message reaches coordinator second time it is discarded (it finishes the
 * second pass).
 */
public class GridTcpDiscoveryHeartbeatMessage extends GridTcpDiscoveryAbstractMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Map to store nodes metrics. */
    @GridToStringExclude
    private Map<UUID, MetricsSet> metrics;

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

        metrics = U.newHashMap(1);
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
        assert !this.metrics.containsKey(nodeId);

        this.metrics.put(nodeId, new MetricsSet(metrics));
    }

    /**
     * Sets metrics for a client node.
     *
     * @param nodeId Server node ID.
     * @param clientNodeId Client node ID.
     * @param metrics Node metrics.
     */
    public void setClientMetrics(UUID nodeId, UUID clientNodeId, @Nullable GridNodeMetrics metrics) {
        assert nodeId != null;
        assert clientNodeId != null;
        assert this.metrics.containsKey(nodeId);

        if (metrics != null) {
            MetricsSet set = this.metrics.get(nodeId);

            set.addClientMetrics(clientNodeId, metrics);
        }

        clientNodeIds.add(clientNodeId);
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
    public Map<UUID, MetricsSet> metrics() {
        return metrics;
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

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        out.writeInt(metrics.size());

        if (!metrics.isEmpty()) {
            for (Map.Entry<UUID, MetricsSet> e : metrics.entrySet()) {
                U.writeUuid(out, e.getKey());
                out.writeObject(e.getValue());
            }
        }

        U.writeCollection(out, clientNodeIds);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        int metricsSize = in.readInt();

        metrics = U.newHashMap(metricsSize);

        for (int i = 0; i < metricsSize; i++)
            metrics.put(U.readUuid(in), (MetricsSet)in.readObject());

        clientNodeIds = U.readCollection(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridTcpDiscoveryHeartbeatMessage.class, this, "super", super.toString());
    }

    /**
     * @param metrics Metrics.
     * @return Serialized metrics.
     */
    private static byte[] serializeMetrics(GridNodeMetrics metrics) {
        assert metrics != null;

        byte[] buf = new byte[GridDiscoveryMetricsHelper.METRICS_SIZE];

        serialize(buf, 0, metrics);

        return buf;
    }

    /**
     * @param nodeId Node ID.
     * @param metrics Metrics.
     * @return Serialized metrics.
     */
    private static byte[] serializeMetrics(UUID nodeId, GridNodeMetrics metrics) {
        assert nodeId != null;
        assert metrics != null;

        byte[] buf = new byte[16 + GridDiscoveryMetricsHelper.METRICS_SIZE];

        U.longToBytes(nodeId.getMostSignificantBits(), buf, 0);
        U.longToBytes(nodeId.getLeastSignificantBits(), buf, 8);

        serialize(buf, 16, metrics);

        return buf;
    }

    /**
     */
    @SuppressWarnings("PublicInnerClass")
    public static class MetricsSet implements Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Metrics. */
        private byte[] metrics;

        /** Client metrics. */
        private Collection<byte[]> clientMetrics;

        /**
         * @param metrics Metrics.
         */
        public MetricsSet(GridNodeMetrics metrics) {
            assert metrics != null;

            this.metrics = serializeMetrics(metrics);
        }

        /**
         * @return Deserialized metrics.
         */
        public GridNodeMetrics metrics() {
            return deserialize(metrics, 0);
        }

        /**
         * @return Client metrics.
         */
        public Collection<T2<UUID, GridNodeMetrics>> clientMetrics() {
            return F.viewReadOnly(clientMetrics, new C1<byte[], T2<UUID, GridNodeMetrics>>() {
                @Override public T2<UUID, GridNodeMetrics> apply(byte[] bytes) {
                    UUID nodeId = new UUID(U.bytesToLong(bytes, 0), U.bytesToLong(bytes, 8));

                    return new T2<>(nodeId, deserialize(bytes, 16));
                }
            });
        }

        /**
         * @param nodeId Client node ID.
         * @param metrics Client metrics.
         */
        private void addClientMetrics(UUID nodeId, GridNodeMetrics metrics) {
            assert nodeId != null;
            assert metrics != null;

            if (clientMetrics == null)
                clientMetrics = new ArrayList<>();

            clientMetrics.add(serializeMetrics(nodeId, metrics));
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            U.writeByteArray(out, metrics);

            out.writeInt(clientMetrics != null ? clientMetrics.size() : -1);

            for (byte[] arr : clientMetrics)
                U.writeByteArray(out, arr);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            metrics = U.readByteArray(in);

            int clientMetricsSize = in.readInt();

            if (clientMetricsSize >= 0) {
                clientMetrics = new ArrayList<>(clientMetricsSize);

                for (int i = 0; i < clientMetricsSize; i++)
                    clientMetrics.add(U.readByteArray(in));
            }
        }
    }
}
