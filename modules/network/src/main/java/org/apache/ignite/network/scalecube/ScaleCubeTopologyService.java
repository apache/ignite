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

import io.scalecube.cluster.Member;
import io.scalecube.cluster.membership.MembershipEvent;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.network.AbstractTopologyService;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.TopologyEventHandler;
import org.apache.ignite.network.TopologyService;

/**
 * Implementation of {@link TopologyService} based on ScaleCube.
 */
final class ScaleCubeTopologyService extends AbstractTopologyService {
    /** Local member node. */
    private ClusterNode localMember;

    /** Topology members. */
    private final Map<String, ClusterNode> members = new ConcurrentHashMap<>();

    /**
     * Sets the ScaleCube's local {@link Member}.
     */
    void setLocalMember(Member member) {
        this.localMember = fromMember(member);

        this.members.put(localMember.name(), localMember);
    }

    /**
     * Delegates the received topology event to the registered event handlers.
     */
    void onMembershipEvent(MembershipEvent event) {
        ClusterNode member = fromMember(event.member());

        String memberName = member.name();

        switch (event.type()) {
            case ADDED:
                members.put(memberName, member);

                fireAppearedEvent(member);

                break;

            case LEAVING:
                members.remove(memberName);

                fireDisappearedEvent(member);

                break;

            case REMOVED:
                // In case if member left non-gracefully, without sending LEAVING event.
                if (members.remove(memberName) != null)
                    fireDisappearedEvent(member);

                break;

            case UPDATED:
                // No-op.
                break;

            default:
                throw new IgniteInternalException("This event is not supported: event = " + event);

        }
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
        return localMember;
    }

    /** {@inheritDoc} */
    @Override public Collection<ClusterNode> allMembers() {
        return Collections.unmodifiableCollection(members.values());
    }

    /**
     * Converts the given {@link Member} to a {@link ClusterNode}.
     */
    private static ClusterNode fromMember(Member member) {
        return new ClusterNode(member.alias(), member.address().host(), member.address().port());
    }
}
