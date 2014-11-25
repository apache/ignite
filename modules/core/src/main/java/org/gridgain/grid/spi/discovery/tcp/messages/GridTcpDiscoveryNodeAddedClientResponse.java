/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.discovery.tcp.messages;

import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;

/**
 * Response from client for {@link GridTcpDiscoveryNodeAddedMessage}.
 */
public class GridTcpDiscoveryNodeAddedClientResponse extends GridTcpDiscoveryAbstractMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Node added message ID. */
    private GridUuid nodeAddedMsgId;

    /** Discovery data from client node. */
    private List<Object> discoData;

    /**
     * Default no-arg constructor for {@link Externalizable} interface.
     */
    public GridTcpDiscoveryNodeAddedClientResponse() {
        // No-op.
    }

    /**
     * @param nodeAddedMsgId Node added message ID.
     * @param creatorNodeId Creator node ID.
     * @param discoData Discovery data from client node.
     */
    public GridTcpDiscoveryNodeAddedClientResponse(UUID creatorNodeId, GridUuid nodeAddedMsgId,
        List<Object> discoData) {
        super(creatorNodeId);

        this.nodeAddedMsgId = nodeAddedMsgId;
        this.discoData = discoData;
    }

    /**
     * @return Node added message ID.
     */
    public GridUuid nodeAddedMessageId() {
        return nodeAddedMsgId;
    }

    /**
     * @return Discovery data from client node.
     */
    public List<Object> discoveryData() {
        return discoData;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        U.writeGridUuid(out, nodeAddedMsgId);
        out.writeObject(discoData);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        nodeAddedMsgId = U.readGridUuid(in);
        discoData = (List<Object>)in.readObject();
    }
}
