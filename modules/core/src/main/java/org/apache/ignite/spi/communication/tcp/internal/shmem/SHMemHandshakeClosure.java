/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.spi.communication.tcp.internal.shmem;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketTimeoutException;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.lang.IgniteInClosure2X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.internal.ClusterStateProvider;
import org.apache.ignite.spi.communication.tcp.messages.NodeIdMessage;

import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.NODE_ID_MSG_TYPE;
import static org.apache.ignite.spi.communication.tcp.internal.CommunicationTcpUtils.writeMessageType;

/**
 * Does handshake for a shmem mode.
 */
public class SHMemHandshakeClosure extends IgniteInClosure2X<InputStream, OutputStream> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Logger. */
    private final IgniteLogger log;

    /** Rmt node id. */
    private final UUID rmtNodeId;

    /** State provider. */
    private final ClusterStateProvider stateProvider;

    /** Local node supplier. */
    private final Supplier<ClusterNode> locNodeSupplier;

    /**
     * @param log Logger.
     * @param rmtNodeId Remote node ID.
     * @param stateProvider Cluster state provider.
     * @param locNodeSupplier Local node getter.
     */
    public SHMemHandshakeClosure(
        IgniteLogger log,
        UUID rmtNodeId,
        ClusterStateProvider stateProvider,
        Supplier<ClusterNode> locNodeSupplier
    ) {
        this.log = log;
        this.rmtNodeId = rmtNodeId;
        this.stateProvider = stateProvider;
        this.locNodeSupplier = locNodeSupplier;
    }

    /** {@inheritDoc} */
    @Override public void applyx(InputStream in, OutputStream out) throws IgniteCheckedException {
        try {
            // Handshake.
            byte[] b = new byte[NodeIdMessage.MESSAGE_FULL_SIZE];

            int n = 0;

            while (n < NodeIdMessage.MESSAGE_FULL_SIZE) {
                int cnt = in.read(b, n, NodeIdMessage.MESSAGE_FULL_SIZE - n);

                if (cnt < 0)
                    throw new IgniteCheckedException("Failed to get remote node ID (end of stream reached)");

                n += cnt;
            }

            // First 4 bytes are for length.
            UUID id = U.bytesToUuid(b, Message.DIRECT_TYPE_SIZE);

            if (!rmtNodeId.equals(id))
                throw new IgniteCheckedException("Remote node ID is not as expected [expected=" + rmtNodeId +
                    ", rcvd=" + id + ']');
            else if (log.isDebugEnabled())
                log.debug("Received remote node ID: " + id);
        }
        catch (SocketTimeoutException e) {
            throw new IgniteCheckedException("Failed to perform handshake due to timeout (consider increasing " +
                "'connectionTimeout' configuration property).", e);
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Failed to perform handshake.", e);
        }

        try {
            ClusterNode locNode = locNodeSupplier.get();

            if (locNode == null)
                throw new IgniteSpiException("Local node has not been started or fully initialized " +
                    "[isStopping=" + stateProvider.isStopping() + ']');

            UUID id = locNode.id();

            NodeIdMessage msg = new NodeIdMessage(id);

            out.write(U.IGNITE_HEADER);
            writeMessageType(out, NODE_ID_MSG_TYPE);
            out.write(msg.nodeIdBytes());

            out.flush();

            if (log.isDebugEnabled())
                log.debug("Sent local node ID [locNodeId=" + id + ", rmtNodeId=" + rmtNodeId + ']');
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Failed to perform handshake.", e);
        }
    }
}
