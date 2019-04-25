/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.spi.discovery.tcp.messages;

import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Message telling joining node that it has loopback problem (misconfiguration).
 * This means that remote node is configured to use loopback address, but joining node is not, or vise versa.
 */
public class TcpDiscoveryLoopbackProblemMessage extends TcpDiscoveryAbstractMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Remote node addresses. */
    private final Collection<String> addrs;

    /** Remote node host names. */
    private final Collection<String> hostNames;

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
    @Override public String toString() {
        return S.toString(TcpDiscoveryLoopbackProblemMessage.class, this, "super", super.toString());
    }
}