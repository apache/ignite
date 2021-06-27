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
package org.apache.ignite.network.scalecube;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import io.scalecube.cluster.Member;
import io.scalecube.cluster.membership.MembershipEvent;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.network.AbstractTopologyService;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.TopologyEventHandler;
import org.apache.ignite.network.TopologyService;

/**
 * Implementation of {@link TopologyService} based on ScaleCube.
 */
final class ScaleCubeTopologyService extends AbstractTopologyService {
    /** Logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(ScaleCubeTopologyService.class);

    /** Local member node. */
    private ClusterNode localMember;

    /** Topology members. */
    private final ConcurrentMap<NetworkAddress, ClusterNode> members = new ConcurrentHashMap<>();

    /**
     * Sets the ScaleCube's local {@link Member}.
     *
     * @param member Local member.
     */
    void setLocalMember(Member member) {
        localMember = fromMember(member);

        // emit an artificial event as if the local member has joined the topology (ScaleCube doesn't do that)
        onMembershipEvent(MembershipEvent.createAdded(member, null, System.currentTimeMillis()));
    }

    /**
     * Delegates the received topology event to the registered event handlers.
     *
     * @param event Membership event.
     */
    void onMembershipEvent(MembershipEvent event) {
        ClusterNode member = fromMember(event.member());

        if (event.isAdded()) {
            members.put(member.address(), member);

            LOG.info("Node joined: " + member);

            fireAppearedEvent(member);
        }
        else if (event.isRemoved()) {
            members.compute(member.address(), // Ignore stale remove event.
                (k, v) -> v.id().equals(member.id()) ? null : v);

            LOG.info("Node left: " + member);

            fireDisappearedEvent(member);
        }

        StringBuilder snapshotMsg = new StringBuilder("Topology snapshot [nodes=").append(members.size()).append("]\n");

        for (ClusterNode node : members.values()) {
            snapshotMsg.append("  ^-- ").append(node).append('\n');
        }

        LOG.info(snapshotMsg.toString().trim());
    }

    /**
     * Fire a cluster member appearance event.
     *
     * @param member Appeared cluster member.
     */
    private void fireAppearedEvent(ClusterNode member) {
        for (TopologyEventHandler handler : getEventHandlers())
            handler.onAppeared(member);
    }

    /**
     * Fire a cluster member disappearance event.
     *
     * @param member Disappeared cluster member.
     */
    private void fireDisappearedEvent(ClusterNode member) {
        for (TopologyEventHandler handler : getEventHandlers())
            handler.onDisappeared(member);
    }

    /** {@inheritDoc} */
    @Override public ClusterNode localMember() {
        assert localMember != null : "Cluster has not been started";

        return localMember;
    }

    /** {@inheritDoc} */
    @Override public Collection<ClusterNode> allMembers() {
        return Collections.unmodifiableCollection(members.values());
    }

    /** {@inheritDoc} */
    @Override public ClusterNode getByAddress(NetworkAddress addr) {
        return members.get(addr);
    }

    /**
     * Converts the given {@link Member} to a {@link ClusterNode}.
     */
    private static ClusterNode fromMember(Member member) {
        var addr = new NetworkAddress(member.address().host(), member.address().port());

        return new ClusterNode(member.id(), member.alias(), addr);
    }
}
