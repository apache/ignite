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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObjectInput;
import org.apache.ignite.internal.visor.VisorDataTransferObjectOutput;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Lightweight counterpart for {@link org.apache.ignite.events.JobEvent} .
 */
public class VisorGridJobEvent extends VisorGridEvent {
    /** */
    private static final long serialVersionUID = 0L;

    /** Name of the task that triggered the event. */
    private String taskName;

    /** Name of task class that triggered the event. */
    private String taskClsName;

    /** Task session ID of the task that triggered the event. */
    private IgniteUuid taskSesId;

    /** Job ID. */
    private IgniteUuid jobId;

    /**
     * Default constructor.
     */
    public VisorGridJobEvent() {
        // No-op.
    }

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
     * @param jobId Job ID.
     */
    public VisorGridJobEvent(
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
        IgniteUuid jobId
    ) {
        super(typeId, id, name, nid, ts, msg, shortDisplay);

        this.taskName = taskName;
        this.taskClsName = taskClsName;
        this.taskSesId = taskSesId;
        this.jobId = jobId;
    }

    /**
     * @return Name of the task that triggered the event.
     */
    public String getTaskName() {
        return taskName;
    }

    /**
     * @return Name of task class that triggered the event.
     */
    public String getTaskClassName() {
        return taskClsName;
    }

    /**
     * @return Task session ID of the task that triggered the event.
     */
    public IgniteUuid getTaskSessionId() {
        return taskSesId;
    }

    /**
     * @return Job ID.
     */
    public IgniteUuid getJobId() {
        return jobId;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        try (VisorDataTransferObjectOutput dtout = new VisorDataTransferObjectOutput(out)) {
            dtout.writeByte(super.getProtocolVersion());

            super.writeExternalData(dtout);
        }

        U.writeString(out, taskName);
        U.writeString(out, taskClsName);
        U.writeIgniteUuid(out, taskSesId);
        U.writeIgniteUuid(out, jobId);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        try (VisorDataTransferObjectInput dtin = new VisorDataTransferObjectInput(in)) {
            super.readExternalData(dtin.readByte(), dtin);
        }

        taskName = U.readString(in);
        taskClsName = U.readString(in);
        taskSesId = U.readIgniteUuid(in);
        jobId = U.readIgniteUuid(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorGridJobEvent.class, this);
    }
}
