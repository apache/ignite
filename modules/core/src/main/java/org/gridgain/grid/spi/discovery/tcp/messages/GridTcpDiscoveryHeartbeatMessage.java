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
    private Map<UUID, T2<byte[], Map<UUID, byte[]>>> metrics;

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
        assert !this.metrics.containsKey(nodeId);

        this.metrics.put(nodeId, new T2<byte[], Map<UUID, byte[]>>(serializeMetrics(metrics), null));
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
            T2<byte[], Map<UUID, byte[]>> t = this.metrics.get(nodeId);

            Map<UUID, byte[]> map = t.get2();

            if (map == null)
                t.set2(map = new HashMap<>());

            map.put(clientNodeId, serializeMetrics(metrics));
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
    public Map<UUID, GridNodeMetrics> metrics() {
        final Iterator<Map.Entry<UUID, T2<byte[], Map<UUID, byte[]>>>> it = metrics.entrySet().iterator();

        return new AbstractMap<UUID, GridNodeMetrics>() {
            @Override public Set<Entry<UUID, GridNodeMetrics>> entrySet() {
                return new AbstractSet<Entry<UUID, GridNodeMetrics>>() {
                    @Override public Iterator<Entry<UUID, GridNodeMetrics>> iterator() {
                        return new Iterator<Entry<UUID, GridNodeMetrics>>() {
                            private Entry<UUID, T2<byte[], Map<UUID, byte[]>>> cur = it.hasNext() ? it.next() : null;

                            private Iterator<Entry<UUID, byte[]>> clientsIt;

                            @Override public boolean hasNext() {
                                return clientsIt == null ? cur != null : clientsIt.hasNext() || it.hasNext();
                            }

                            @Override public Entry<UUID, GridNodeMetrics> next() {
                                if (clientsIt == null) {
                                    if (cur == null)
                                        throw new NoSuchElementException();

                                    Map<UUID, byte[]> map = cur.getValue().get2();

                                    clientsIt = map != null ? map.entrySet().iterator() :
                                        Collections.<Entry<UUID, byte[]>>emptyIterator();

                                    return new T2<>(cur.getKey(), deserialize(cur.getValue().get1(), 0));
                                }
                                else {
                                    if (clientsIt.hasNext()) {
                                        Entry<UUID, byte[]> nextClient = clientsIt.next();

                                        return new T2<>(nextClient.getKey(), deserialize(nextClient.getValue(), 0));
                                    }
                                    else {
                                        clientsIt = null;

                                        cur = it.hasNext() ? it.next() : null;

                                        return next();
                                    }
                                }
                            }

                            @Override public void remove() {
                                throw new UnsupportedOperationException();
                            }
                        };
                    }

                    @Override public int size() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
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
            for (Map.Entry<UUID, T2<byte[], Map<UUID, byte[]>>> e : metrics.entrySet()) {
                U.writeUuid(out, e.getKey());
                U.writeByteArray(out, e.getValue().get1());
                U.writeMap(out, e.getValue().get2());
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
            metrics.put(U.readUuid(in), new T2<>(U.readByteArray(in), U.<UUID, byte[]>readMap(in)));

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
    private byte[] serializeMetrics(GridNodeMetrics metrics) {
        assert metrics != null;

        byte[] buf = new byte[GridDiscoveryMetricsHelper.METRICS_SIZE];

        serialize(buf, 0, metrics);

        return buf;
    }
}
