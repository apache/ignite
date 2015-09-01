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
 * Lightweight counterpart for {@link org.apache.ignite.events.TaskEvent}.
 */
public class VisorGridTaskEvent extends VisorGridEvent {
    /** */
    private static final long serialVersionUID = 0L;

    /** Name of the task that triggered the event. */
    private final String taskName;

    /** Name of task class that triggered the event. */
    private final String taskClsName;

    /** Task session ID. */
    private final IgniteUuid taskSesId;

    /** Whether task was created for system needs. */
    private final boolean internal;

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
     * @param taskName Name of the task that triggered the event.
     * @param taskClsName Name of task class that triggered the event.
     * @param taskSesId Task session ID of the task that triggered the event.
     * @param internal Whether task was created for system needs.
     */
    public VisorGridTaskEvent(
        int typeId,
        IgniteUuid id,
        String name,
        UUID nid,
        long ts,
        @Nullable String msg,
        String shortDisplay,
        String taskName,
        String taskClsName,
        IgniteUuid taskSesId,
        boolean internal
    ) {
        super(typeId, id, name, nid, ts, msg, shortDisplay);

        this.taskName = taskName;
        this.taskClsName = taskClsName;
        this.taskSesId = taskSesId;
        this.internal = internal;
    }

    /**
     * @return Name of the task that triggered the event.
     */
    public String taskName() {
        return taskName;
    }

    /**
     * @return Name of task class that triggered the event.
     */
    public String taskClassName() {
        return taskClsName;
    }

    /**
     * @return Task session ID.
     */
    public IgniteUuid taskSessionId() {
        return taskSesId;
    }

    /**
     * @return Whether task was created for system needs.
     */
    public boolean internal() {
        return internal;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorGridTaskEvent.class, this);
    }
}