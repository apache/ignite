/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.discovery.tcp.messages;

import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.net.*;
import java.util.*;

/**
 * Message telling joining node that its authentication failed on coordinator.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridTcpDiscoveryAuthFailedMessage extends GridTcpDiscoveryAbstractMessage {
    /** Coordinator address. */
    private InetAddress addr;

    /**
     * Public default no-arg constructor for {@link Externalizable} interface.
     */
    public GridTcpDiscoveryAuthFailedMessage() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param creatorNodeId Creator node ID.
     * @param addr Coordinator address.
     */
    public GridTcpDiscoveryAuthFailedMessage(UUID creatorNodeId, InetAddress addr) {
        super(creatorNodeId);

        this.addr = addr;
    }

    /**
     * @return Coordinator address.
     */
    public InetAddress address() {
        return addr;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        U.writeByteArray(out, addr.getAddress());
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        addr = InetAddress.getByAddress(U.readByteArray(in));
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridTcpDiscoveryAuthFailedMessage.class, this, "super", super.toString());
    }
}
