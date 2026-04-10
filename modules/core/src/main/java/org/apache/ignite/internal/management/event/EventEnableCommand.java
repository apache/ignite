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

package org.apache.ignite.internal.management.event;

import java.util.Collection;
import java.util.function.Consumer;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.management.api.ComputeCommand;

import static org.apache.ignite.internal.management.api.CommandUtils.servers;

/** */
public class EventEnableCommand implements ComputeCommand<EventCommandArg, Void> {
    /** {@inheritDoc} */
    @Override public String description() {
        return "Enable events on all server nodes";
    }

    /** {@inheritDoc} */
    @Override public Class<EventEnableCommandArg> argClass() {
        return EventEnableCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<EventManagementTask> taskClass() {
        return EventManagementTask.class;
    }

    /** {@inheritDoc} */
    @Override public Collection<ClusterNode> nodes(Collection<ClusterNode> nodes, EventCommandArg arg) {
        return servers(nodes);
    }

    /** {@inheritDoc} */
    @Override public void printResult(EventCommandArg arg, Void res, Consumer<String> printer) {
        printer.accept("Events were enabled [evts=" + String.join(", ", arg.events()) + ']');
    }

    /** */
    public static class EventEnableCommandArg extends EventCommandArg {
        /** */
        private static final long serialVersionUID = 0;
    }
}
