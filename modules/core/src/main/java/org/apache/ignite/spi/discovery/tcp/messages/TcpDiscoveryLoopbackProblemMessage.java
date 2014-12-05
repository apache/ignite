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
 * Message telling joining node that it has loopback problem (misconfiguration).
 * This means that remote node is configured to use loopback address, but joining node is not, or vise versa.
 */
public class TcpDiscoveryLoopbackProblemMessage extends TcpDiscoveryAbstractMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Remote node addresses. */
    private Collection<String> addrs;

    /** Remote node host names. */
    private Collection<String> hostNames;

    /**
     * Public default no-arg constructor for {@link Externalizable} interface.
     */
    public TcpDiscoveryLoopbackProblemMessage() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param creatorNodeId Creator node ID.
     * @param addrs Remote node addresses.
     * @param hostNames Remote node host names.
     */
    public TcpDiscoveryLoopbackProblemMessage(UUID creatorNodeId, Collection<String> addrs,
                                              Collection<String> hostNames) {
        super(creatorNodeId);

        this.addrs = addrs;
        this.hostNames = hostNames;
    }

    /**
     * @return Remote node addresses.
     */
    public Collection<String> addresses() {
        return addrs;
    }

    /**
     * @return Remote node host names.
     */
    public Collection<String> hostNames() {
        return hostNames;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        U.writeCollection(out, addrs);
        U.writeCollection(out, hostNames);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        addrs = U.readCollection(in);
        hostNames = U.readCollection(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpDiscoveryLoopbackProblemMessage.class, this, "super", super.toString());
    }
}
