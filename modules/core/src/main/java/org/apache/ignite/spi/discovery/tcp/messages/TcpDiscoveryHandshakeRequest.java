/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.spi.discovery.tcp.messages;

import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;

/**
 * Handshake request.
 */
public class TcpDiscoveryHandshakeRequest extends TcpDiscoveryAbstractMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Public default no-arg constructor for {@link Externalizable} interface.
     */
    public TcpDiscoveryHandshakeRequest() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param creatorNodeId Creator node ID.
     */
    public TcpDiscoveryHandshakeRequest(UUID creatorNodeId) {
        super(creatorNodeId);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpDiscoveryHandshakeRequest.class, this, "super", super.toString());
    }
}
