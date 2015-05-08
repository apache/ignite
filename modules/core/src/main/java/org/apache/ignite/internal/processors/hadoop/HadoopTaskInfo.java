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

package org.apache.ignite.internal.processors.hadoop;

import org.apache.ignite.internal.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;

/**
 * Task info.
 */
public class HadoopTaskInfo implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private HadoopTaskType type;

    /** */
    private HadoopJobId jobId;

    /** */
    private int taskNum;

    /** */
    private int attempt;

    /** */
    private HadoopInputSplit inputSplit;

    /**
     * For {@link Externalizable}.
     */
    public HadoopTaskInfo() {
        // No-op.
    }

    /**
     * Creates new task info.
     *
     * @param type Task type.
     * @param jobId Job id.
     * @param taskNum Task number.
     * @param attempt Attempt for this task.
     * @param inputSplit Input split.
     */
    public HadoopTaskInfo(HadoopTaskType type, HadoopJobId jobId, int taskNum, int attempt,
        @Nullable HadoopInputSplit inputSplit) {
        this.type = type;
        this.jobId = jobId;
        this.taskNum = taskNum;
        this.attempt = attempt;
        this.inputSplit = inputSplit;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeByte(type.ordinal());
        out.writeObject(jobId);
        out.writeInt(taskNum);
        out.writeInt(attempt);
        out.writeObject(inputSplit);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        type = HadoopTaskType.fromOrdinal(in.readByte());
        jobId = (HadoopJobId)in.readObject();
        taskNum = in.readInt();
        attempt = in.readInt();
        inputSplit = (HadoopInputSplit)in.readObject();
    }

    /**
     * @return Type.
     */
    public HadoopTaskType type() {
        return type;
    }

    /**
     * @return Job id.
     */
    public HadoopJobId jobId() {
        return jobId;
    }

    /**
     * @return Task number.
     */
    public int taskNumber() {
        return taskNum;
    }

    /**
     * @return Attempt.
     */
    public int attempt() {
        return attempt;
    }

    /**
     * @return Input split.
     */
    @Nullable public HadoopInputSplit inputSplit() {
        return inputSplit;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof HadoopTaskInfo))
            return false;

        HadoopTaskInfo that = (HadoopTaskInfo)o;

        return attempt == that.attempt && taskNum == that.taskNum && jobId.equals(that.jobId) && type == that.type;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = type.hashCode();

        res = 31 * res + jobId.hashCode();
        res = 31 * res + taskNum;
        res = 31 * res + attempt;

        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(HadoopTaskInfo.class, this);
    }
}
