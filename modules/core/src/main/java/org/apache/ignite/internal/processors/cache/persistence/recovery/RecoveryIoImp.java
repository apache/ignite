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

package org.apache.ignite.internal.processors.cache.persistence.recovery;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.discovery.ConsistentIdMapper;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.eventstorage.DiscoveryEventListener;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.GridTopic.TOPIC_RECOVERY;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;

/**
 *
 */
public class RecoveryIoImp implements RecoveryIo, GridMessageListener, DiscoveryEventListener {
    /** */
    private final GridKernalContext ctx;

    /** */
    private final ConsistentIdMapper idMapper;

    /** */
    private volatile IgniteBiInClosure<String, Message> msgHandler;

    /** */
    private volatile IgniteInClosure<String> nodeLeftHandler;

    /** */
    private final IgniteLogger log;

    /**
     *
     */
    public RecoveryIoImp(GridKernalContext ctx, @Nullable IgniteLogger log) {
        idMapper = new ConsistentIdMapper(ctx.discovery());

        this.log = log;
        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override public void send(String constId, Message msg) throws IgniteCheckedException {
        UUID nodeId = idMapper.mapToUUID(constId);

        assert nodeId != null;

        ctx.io().sendToGridTopic(nodeId, TOPIC_RECOVERY, msg, SYSTEM_POOL);
    }

    /** {@inheritDoc} */
    @Override public void receive(final IgniteBiInClosure<String, Message> handler) {
        msgHandler = handler;
    }

    /** {@inheritDoc} */
    @Override public void onNodeLeft(final IgniteInClosure<String> handler) {
        nodeLeftHandler = handler;
    }

    /** {@inheritDoc} */
    @Override public List<String> constIds(AffinityTopologyVersion ver) {
        List<String> constIds = new ArrayList<>();

        for (Object constId : idMapper.mapToConsistentId(ver))
            constIds.add(constId.toString());

        return constIds;
    }

    /** {@inheritDoc} */
    @Override public String localNodeConsistentId() throws IgniteCheckedException {
        try {
            return ctx.pdsFolderResolver().resolveFolders().consistentId().toString();
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Fail resolve consistent id.", e);

            new Thread(
                new Runnable() {
                    @Override public void run() {
                        G.stop(true);
                    }
                }
            ).start();

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
        IgniteBiInClosure<String, Message> handler = msgHandler;

        if (handler != null && msg instanceof Message) {
            String constId = idMapper.mapToConsistentId(
                ctx.discovery().topologyVersionEx(), nodeId
            ).toString();

            handler.apply(constId, (Message)msg);
        }
    }

    /** {@inheritDoc} */
    @Override public void onEvent(DiscoveryEvent evt, DiscoCache discoCache) {
        IgniteInClosure<String> handler = nodeLeftHandler;

        if (handler != null) {
            ClusterNode node = evt.eventNode();

            String constId = idMapper.mapToConsistentId(
                ctx.discovery().topologyVersionEx(), node.id()).toString();

            handler.apply(constId);
        }
    }

    /**
     *
     */
    public void reset() {
        msgHandler = null;
        nodeLeftHandler = null;
    }
}
