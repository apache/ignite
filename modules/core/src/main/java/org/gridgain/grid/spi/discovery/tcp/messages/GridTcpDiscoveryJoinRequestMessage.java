/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.discovery.tcp.messages;

import org.gridgain.grid.spi.discovery.tcp.internal.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;

/**
 * Initial message sent by a node that wants to enter topology.
 * Sent to random node during SPI start. Then forwarded directly to coordinator.
 */
public class GridTcpDiscoveryJoinRequestMessage extends GridTcpDiscoveryAbstractMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** New node that wants to join the topology. */
    private GridTcpDiscoveryNode node;

    /** Discovery data. */
    private List<Object> discoData;

    /** Responded flag. */
    private boolean responded;

    /** Client reconnection flag. */
    private boolean clientRecon;

    /**
     * Public default no-arg constructor for {@link Externalizable} interface.
     */
    public GridTcpDiscoveryJoinRequestMessage() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param node New node that wants to join.
     * @param discoData Discovery data.
     */
    public GridTcpDiscoveryJoinRequestMessage(GridTcpDiscoveryNode node, List<Object> discoData) {
        super(node.id());

        this.node = node;
        this.discoData = discoData;
    }

    /**
     * Gets new node that wants to join the topology.
     *
     * @return Node that wants to join the topology.
     */
    public GridTcpDiscoveryNode node() {
        return node;
    }

    /**
     * @return Discovery data.
     */
    public List<Object> discoveryData() {
        return discoData;
    }

    /**
     * @return {@code true} flag.
     */
    public boolean responded() {
        return responded;
    }

    /**
     * @param responded Responded flag.
     */
    public void responded(boolean responded) {
        this.responded = responded;
    }

    /**
     * @return Client reconnection flag.
     */
    public boolean clientReconnect() {
        return clientRecon;
    }

    /**
     * @param clientRecon Client reconnection flag.
     */
    public void clientReconnect(boolean clientRecon) {
        this.clientRecon = clientRecon;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        out.writeObject(node);
        out.writeObject(discoData);
        out.writeBoolean(responded);
        out.writeBoolean(clientRecon);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        node = (GridTcpDiscoveryNode)in.readObject();
        discoData = (List<Object>)in.readObject();
        responded = in.readBoolean();
        clientRecon = in.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridTcpDiscoveryJoinRequestMessage.class, this, "super", super.toString());
    }
}
