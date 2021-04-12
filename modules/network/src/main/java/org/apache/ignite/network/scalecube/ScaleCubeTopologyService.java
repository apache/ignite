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
import java.util.stream.Collectors;
import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.Member;
import io.scalecube.cluster.membership.MembershipEvent;
import org.apache.ignite.network.AbstractTopologyService;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.TopologyEventHandler;
import org.apache.ignite.network.TopologyService;

/**
 * Implementation of {@link TopologyService} based on ScaleCube.
 */
final class ScaleCubeTopologyService extends AbstractTopologyService {
    /** Inner representation a ScaleCube cluster. */
    private Cluster cluster;

    /**
     * Sets the ScaleCube's {@link Cluster}. Needed for cyclic dependency injection.
     */
    void setCluster(Cluster cluster) {
        this.cluster = cluster;
    }

    /**
     * Delegates the received topology event to the registered event handlers.
     */
    void fireEvent(MembershipEvent event) {
        ClusterNode member = fromMember(event.member());
        for (TopologyEventHandler handler : getEventHandlers()) {
            switch (event.type()) {
                case ADDED:
                    handler.onAppeared(member);
                    break;
                case LEAVING:
                case REMOVED:
                    handler.onDisappeared(member);
                    break;
                case UPDATED:
                    // do nothing
                    break;
                default:
                    throw new RuntimeException("This event is not supported: event = " + event);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public ClusterNode localMember() {
        return fromMember(cluster.member());
    }

    /** {@inheritDoc} */
    @Override public Collection<ClusterNode> allMembers() {
        return cluster.members().stream()
            .map(ScaleCubeTopologyService::fromMember)
            .collect(Collectors.toList());
    }

    /**
     * Converts the given {@link Member} to a {@link ClusterNode}.
     */
    private static ClusterNode fromMember(Member member) {
        return new ClusterNode(member.alias(), member.address().host(), member.address().port());
    }
}
