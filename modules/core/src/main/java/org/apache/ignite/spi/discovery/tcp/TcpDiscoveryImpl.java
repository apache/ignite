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

package org.apache.ignite.spi.discovery.tcp;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.spi.*;
import org.apache.ignite.spi.discovery.*;
import org.apache.ignite.spi.discovery.tcp.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 *
 */
abstract class TcpDiscoveryImpl {
    /** Response OK. */
    protected static final int RES_OK = 1;

    /** Response CONTINUE JOIN. */
    protected static final int RES_CONTINUE_JOIN = 100;

    /** Response WAIT. */
    protected static final int RES_WAIT = 200;

    /** */
    protected final TcpDiscoverySpi spi;

    /** */
    protected final IgniteLogger log;

    /** */
    protected TcpDiscoveryNode locNode;

    /**
     * @param spi Adapter.
     */
    TcpDiscoveryImpl(TcpDiscoverySpi spi) {
        this.spi = spi;

        log = spi.log;
    }

    /**
     * @return Local node ID.
     */
    public UUID getLocalNodeId() {
        return spi.getLocalNodeId();
    }

    /**
     * @param msg Error message.
     * @param e Exception.
     */
    protected void onException(String msg, Exception e){
        spi.getExceptionRegistry().onException(msg, e);
    }

    /**
     * @param log Logger.
     */
    public abstract void dumpDebugInfo(IgniteLogger log);

    /**
     * @return SPI state string.
     */
    public abstract String getSpiState();

    /**
     * @return Message worker queue current size.
     */
    public abstract int getMessageWorkerQueueSize();

    /**
     * @return Coordinator ID.
     */
    public abstract UUID getCoordinator();

    /**
     * @return Collection of remote nodes.
     */
    public abstract Collection<ClusterNode> getRemoteNodes();

    /**
     * @param nodeId Node id.
     * @return Node with given ID or {@code null} if node is not found.
     */
    @Nullable public abstract ClusterNode getNode(UUID nodeId);

    /**
     * @param nodeId Node id.
     * @return {@code true} if node alive, {@code false} otherwise.
     */
    public abstract boolean pingNode(UUID nodeId);

    /**
     * Tells discovery SPI to disconnect from topology.
     *
     * @throws IgniteSpiException If failed.
     */
    public abstract void disconnect() throws IgniteSpiException;

    /**
     * @param msg Message.
     * @throws IgniteException If failed.
     */
    public abstract void sendCustomEvent(DiscoverySpiCustomMessage msg) throws IgniteException;

    /**
     * @param nodeId Node id.
     */
    public abstract void failNode(UUID nodeId);

    /**
     * @param gridName Grid name.
     * @throws IgniteSpiException If failed.
     */
    public abstract void spiStart(@Nullable String gridName) throws IgniteSpiException;

    /**
     * @throws IgniteSpiException If failed.
     */
    public abstract void spiStop() throws IgniteSpiException;

    /**
     * @param spiCtx Spi context.
     * @throws IgniteSpiException If failed.
     */
    public abstract void onContextInitialized0(IgniteSpiContext spiCtx) throws IgniteSpiException;

    /**
     * @param t Thread.
     * @return Status as string.
     */
    protected static String threadStatus(Thread t) {
        if (t == null)
            return "N/A";

        return t.isAlive() ? "alive" : "dead";
    }

    /**
     * <strong>FOR TEST ONLY!!!</strong>
     * <p>
     * Simulates this node failure by stopping service threads. So, node will become
     * unresponsive.
     * <p>
     * This method is intended for test purposes only.
     */
    abstract void simulateNodeFailure();

    /**
     * FOR TEST PURPOSE ONLY!
     */
    public abstract void brakeConnection();

    /**
     * <strong>FOR TEST ONLY!!!</strong>
     *
     * @return Worker thread.
     */
    protected abstract IgniteSpiThread workerThread();

    /**
     * @throws IgniteSpiException If failed.
     */
    @SuppressWarnings("BusyWait")
    protected final void registerLocalNodeAddress() throws IgniteSpiException {
        // Make sure address registration succeeded.
        while (true) {
            try {
                spi.ipFinder.initializeLocalAddresses(locNode.socketAddresses());

                // Success.
                break;
            }
            catch (IllegalStateException e) {
                throw new IgniteSpiException("Failed to register local node address with IP finder: " +
                    locNode.socketAddresses(), e);
            }
            catch (IgniteSpiException e) {
                LT.error(log, e, "Failed to register local node address in IP finder on start " +
                    "(retrying every 2000 ms).");
            }

            try {
                U.sleep(2000);
            }
            catch (IgniteInterruptedCheckedException e) {
                throw new IgniteSpiException("Thread has been interrupted.", e);
            }
        }
    }
}
