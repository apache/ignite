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

package org.apache.ignite.internal.visor.event;

import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Lightweight counterpart for {@link org.apache.ignite.events.DiscoveryEvent}.
 */
public class VisorGridDiscoveryEvent extends VisorGridEvent {
    /** */
    private static final long serialVersionUID = 0L;

    /** Node that caused this event to be generated. */
    private final UUID evtNodeId;

    /** Node address that caused this event to be generated. */
    private final String addr;

    /** If node that caused this event is daemon. */
    private final boolean isDaemon;

    /**
     * Create event with given parameters.
     *
     * @param typeId Event type.
     * @param id Event id.
     * @param name Event name.
     * @param nid Event node ID.
     * @param ts Event timestamp.
     * @param msg Event message.
     * @param shortDisplay Shortened version of {@code toString()} result.
     * @param evtNodeId Event node id.
     * @param addr Event node address.
     * @param isDaemon If event node is daemon on not.
     */
    public VisorGridDiscoveryEvent(
        int typeId,
        IgniteUuid id,
        String name,
        UUID nid,
        long ts,
        @Nullable String msg,
        String shortDisplay,
        UUID evtNodeId,
        String addr,
        boolean isDaemon
    ) {
        super(typeId, id, name, nid, ts, msg, shortDisplay);

        this.evtNodeId = evtNodeId;
        this.addr = addr;
        this.isDaemon = isDaemon;
    }

    /**
     * @return Deployment alias.
     */
    public UUID evtNodeId() {
        return evtNodeId;
    }

    /**
     * @return Node address that caused this event to be generated.
     */
    public String address() {
        return addr;
    }

    /**
     * @return If node that caused this event is daemon.
     */
    public boolean isDaemon() {
        return isDaemon;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorGridDiscoveryEvent.class, this);
    }
}