package org.apache.ignite.spi.discovery.tcp.messages;

import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 *
 */
@TcpDiscoveryEnsureDelivery
//@TcpDiscoveryRedirectToClient
public class TcpDiscoveryNodePartitionsEvictionMessage extends TcpDiscoveryAbstractMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Constructor.
     *
     * @param creatorNodeId ID of the node that is about to leave the topology.
     */
    public TcpDiscoveryNodePartitionsEvictionMessage(UUID creatorNodeId) {
        super(creatorNodeId);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpDiscoveryNodePartitionsEvictionMessage.class, this, "super", super.toString());
    }
}
