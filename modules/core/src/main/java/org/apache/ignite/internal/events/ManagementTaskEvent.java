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

package org.apache.ignite.internal.events;

import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.EventType;
import org.apache.ignite.events.TaskEvent;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Management task started event.
 *
 * @see EventType#EVT_MANAGEMENT_TASK_STARTED
 */
public class ManagementTaskEvent extends TaskEvent {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final VisorTaskArgument<?> arg;

    /**
     * Creates task event with given parameters.
     *
     * @param node Node.
     * @param msg Optional message.
     * @param type Event type.
     * @param sesId Task session ID.
     * @param taskName Task name.
     * @param subjId Subject ID.
     * @param internal Whether current task belongs to Ignite internal tasks.
     * @param taskClsName Name ot the task class.
     */
    public ManagementTaskEvent(ClusterNode node, String msg, int type, IgniteUuid sesId, String taskName,
        String taskClsName, boolean internal, @Nullable UUID subjId, VisorTaskArgument<?> arg) {
        super(node, msg, type, sesId, taskName, taskClsName, internal, subjId);

        this.arg = arg;
    }

    /** @return Task argument. */
    public VisorTaskArgument<?> argument() {
        return arg;
    }
}
