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

package org.apache.ignite.internal.visor.util;

import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DeploymentEvent;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.JobEvent;
import org.apache.ignite.events.TaskEvent;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.event.VisorGridDeploymentEvent;
import org.apache.ignite.internal.visor.event.VisorGridDiscoveryEvent;
import org.apache.ignite.internal.visor.event.VisorGridEvent;
import org.apache.ignite.internal.visor.event.VisorGridJobEvent;
import org.apache.ignite.internal.visor.event.VisorGridTaskEvent;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Mapper from grid event to Visor data transfer object.
 */
public class VisorEventMapper implements IgniteClosure<Event, VisorGridEvent> {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Map grid event to Visor data transfer object.
     *
     * @param evt Grid event.
     * @param type Event's type.
     * @param id Event id.
     * @param name Event name.
     * @param nid Event node ID.
     * @param ts Event timestamp.
     * @param msg Event message.
     * @param shortDisplay Shortened version of {@code toString()} result.
     * @return Visor data transfer object for event.
     */
    protected VisorGridEvent map(Event evt, int type, IgniteUuid id, String name, UUID nid, long ts, String msg,
        String shortDisplay) {
        if (evt instanceof TaskEvent)
            return taskEvent((TaskEvent)evt, type, id, name, nid, ts, msg, shortDisplay);

        if (evt instanceof JobEvent)
            return jobEvent((JobEvent)evt, type, id, name, nid, ts, msg, shortDisplay);

        if (evt instanceof DeploymentEvent)
            return deploymentEvent((DeploymentEvent)evt, type, id, name, nid, ts, msg, shortDisplay);

        if (evt instanceof DiscoveryEvent)
            return discoveryEvent((DiscoveryEvent)evt, type, id, name, nid, ts, msg, shortDisplay);

        return null;
    }

    /**
     * @param te Task event.
     * @param type Event's type.
     * @param id Event id.
     * @param name Event name.
     * @param nid Event node ID.
     * @param ts Event timestamp.
     * @param msg Event message.
     * @param shortDisplay Shortened version of {@code toString()} result.
     * @return Visor data transfer object for event.
     */
    protected VisorGridEvent taskEvent(TaskEvent te, int type, IgniteUuid id, String name, UUID nid, long ts,
        String msg, String shortDisplay) {
        return new VisorGridTaskEvent(type, id, name, nid, ts, msg, shortDisplay,
            te.taskName(), te.taskClassName(), te.taskSessionId(), te.internal());
    }

    /**
     * @param je Job event.
     * @param type Event's type.
     * @param id Event id.
     * @param name Event name.
     * @param nid Event node ID.
     * @param ts Event timestamp.
     * @param msg Event message.
     * @param shortDisplay Shortened version of {@code toString()} result.
     * @return Visor data transfer object for event.
     */
    protected VisorGridEvent jobEvent(JobEvent je, int type, IgniteUuid id, String name, UUID nid, long ts,
        String msg, String shortDisplay) {
        return new VisorGridJobEvent(type, id, name, nid, ts, msg, shortDisplay, je.taskName(), je.taskClassName(),
            je.taskSessionId(), je.jobId());
    }

    /**
     * @param de Deployment event.
     * @param type Event's type.
     * @param id Event id.
     * @param name Event name.
     * @param nid Event node ID.
     * @param ts Event timestamp.
     * @param msg Event message.
     * @param shortDisplay Shortened version of {@code toString()} result.
     * @return Visor data transfer object for event.
     */
    protected VisorGridEvent deploymentEvent(DeploymentEvent de, int type, IgniteUuid id, String name, UUID nid,
        long ts, String msg, String shortDisplay) {
        return new VisorGridDeploymentEvent(type, id, name, nid, ts, msg, shortDisplay, de.alias());
    }

    /**
     * @param de Discovery event.
     * @param type Event's type.
     * @param id Event id.
     * @param name Event name.
     * @param nid Event node ID.
     * @param ts Event timestamp.
     * @param msg Event message.
     * @param shortDisplay Shortened version of {@code toString()} result.
     * @return Visor data transfer object for event.
     */
    protected VisorGridEvent discoveryEvent(DiscoveryEvent de, int type, IgniteUuid id, String name, UUID nid,
        long ts, String msg, String shortDisplay) {
        ClusterNode node = de.eventNode();

        return new VisorGridDiscoveryEvent(type, id, name, nid, ts, msg, shortDisplay, node.id(),
            F.first(node.addresses()), node.isDaemon(), de.topologyVersion());
    }

    /** {@inheritDoc} */
    @Override public VisorGridEvent apply(Event evt) {
        return map(evt, evt.type(), evt.id(), evt.name(), evt.node().id(), evt.timestamp(), evt.message(),
            evt.shortDisplay());
    }
}
