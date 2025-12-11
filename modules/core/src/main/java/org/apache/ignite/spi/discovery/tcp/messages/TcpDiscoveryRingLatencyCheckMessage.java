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

package org.apache.ignite.spi.discovery.tcp.messages;

import java.util.UUID;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.discovery.DiscoveryMessageFactory;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 *
 */
public class TcpDiscoveryRingLatencyCheckMessage extends TcpDiscoveryAbstractMessage implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @Order(value = 5, method = "maximalHops")
    private int maxHops;

    /** */
    @Order(value = 6, method = "currentHops")
    private int curHops;

    /** Empty constructor for {@link DiscoveryMessageFactory}. */
    public TcpDiscoveryRingLatencyCheckMessage() {
        // No-op.
    }

    /**
     * @param creatorNodeId Creator node ID.
     * @param maxHops Max hops for this message.
     */
    public TcpDiscoveryRingLatencyCheckMessage(UUID creatorNodeId, int maxHops) {
        super(creatorNodeId);

        assert maxHops > 0;

        this.maxHops = maxHops;
    }

    /**
     *
     */
    public void onRead() {
        curHops++;
    }

    /** @return Current hops reached. */
    public int currentHops() {
        return curHops;
    }

    /** @param curHop Current hops reached. */
    public void currentHops(int curHop) {
        curHops = curHop;
    }

    /**
     * @return Maximal hops.
     */
    public int maximalHops() {
        return maxHops;
    }

    /** @param maxHops Maximal hops. */
    public void maximalHops(int maxHops) {
        this.maxHops = maxHops;
    }

    /**
     * @return {@code True} if max hops has been reached.
     */
    public boolean maxHopsReached() {
        return curHops == maxHops;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 5;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpDiscoveryRingLatencyCheckMessage.class, this, "super", super.toString());
    }
}
