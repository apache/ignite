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

package org.apache.ignite.internal.visor.node;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVTS_JOB_EXECUTION;
import static org.apache.ignite.events.EventType.EVTS_TASK_EXECUTION;
import static org.apache.ignite.internal.visor.util.VisorTaskUtils.concat;

/**
 * Argument for task returns events data.
 */
public class VisorNodeEventsCollectorTaskArg extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Node local storage key. */
    private String keyOrder;

    /** Arguments for type filter. */
    private int[] typeArg;

    /** Arguments for time filter. */
    private Long timeArg;

    /** Task or job events with task name contains. */
    private String taskName;

    /** Task or job events with session. */
    private IgniteUuid taskSesId;

    /**
     * Default constructor.
     */
    public VisorNodeEventsCollectorTaskArg() {
        // No-op.
    }

    /**
     * @param keyOrder Arguments for node local storage key.
     * @param typeArg Arguments for type filter.
     * @param timeArg Arguments for time filter.
     * @param taskName Arguments for task name filter.
     * @param taskSesId Arguments for task session filter.
     */
    public VisorNodeEventsCollectorTaskArg(@Nullable String keyOrder, @Nullable int[] typeArg,
        @Nullable Long timeArg,
        @Nullable String taskName, @Nullable IgniteUuid taskSesId) {
        this.keyOrder = keyOrder;
        this.typeArg = typeArg;
        this.timeArg = timeArg;
        this.taskName = taskName;
        this.taskSesId = taskSesId;
    }

    /**
     * @param typeArg Arguments for type filter.
     * @param timeArg Arguments for time filter.
     */
    public static VisorNodeEventsCollectorTaskArg createEventsArg(@Nullable int[] typeArg, @Nullable Long timeArg) {
        return new VisorNodeEventsCollectorTaskArg(null, typeArg, timeArg, null, null);
    }

    /**
     * @param timeArg Arguments for time filter.
     * @param taskName Arguments for task name filter.
     * @param taskSesId Arguments for task session filter.
     */
    public static VisorNodeEventsCollectorTaskArg createTasksArg(@Nullable Long timeArg, @Nullable String taskName,
        @Nullable IgniteUuid taskSesId) {
        return new VisorNodeEventsCollectorTaskArg(null, concat(EVTS_JOB_EXECUTION, EVTS_TASK_EXECUTION),
            timeArg, taskName, taskSesId);
    }

    /**
     * @param keyOrder Arguments for node local storage key.
     * @param typeArg Arguments for type filter.
     */
    public static VisorNodeEventsCollectorTaskArg createLogArg(@Nullable String keyOrder, @Nullable int[] typeArg) {
        return new VisorNodeEventsCollectorTaskArg(keyOrder, typeArg, null, null, null);
    }

    /**
     * @return Node local storage key.
     */
    @Nullable public String getKeyOrder() {
        return keyOrder;
    }

    /**
     * @return Arguments for type filter.
     */
    public int[] getTypeArgument() {
        return typeArg;
    }

    /**
     * @return Arguments for time filter.
     */
    public Long getTimeArgument() {
        return timeArg;
    }

    /**
     * @return Task or job events with task name contains.
     */
    public String getTaskName() {
        return taskName;
    }

    /**
     * @return Task or job events with session.
     */
    public IgniteUuid getTaskSessionId() {
        return taskSesId;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, keyOrder);
        out.writeObject(typeArg);
        out.writeObject(timeArg);
        U.writeString(out, taskName);
        U.writeIgniteUuid(out, taskSesId);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        keyOrder = U.readString(in);
        typeArg = (int[])in.readObject();
        timeArg = (Long)in.readObject();
        taskName = U.readString(in);
        taskSesId = U.readIgniteUuid(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorNodeEventsCollectorTaskArg.class, this);
    }
}
