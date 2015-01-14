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

package org.gridgain.grid.kernal.processors.hadoop.taskexecutor.external;

import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.message.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;

/**
 * Message sent from node to child process to start task(s) execution.
 */
public class GridHadoopTaskExecutionRequest implements GridHadoopMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Job ID. */
    @GridToStringInclude
    private GridHadoopJobId jobId;

    /** Job info. */
    @GridToStringInclude
    private GridHadoopJobInfo jobInfo;

    /** Mappers. */
    @GridToStringInclude
    private Collection<GridHadoopTaskInfo> tasks;

    /**
     * @return Job ID.
     */
    public GridHadoopJobId jobId() {
        return jobId;
    }

    /**
     * @param jobId Job ID.
     */
    public void jobId(GridHadoopJobId jobId) {
        this.jobId = jobId;
    }

    /**
     * @return Jon info.
     */
    public GridHadoopJobInfo jobInfo() {
        return jobInfo;
    }

    /**
     * @param jobInfo Job info.
     */
    public void jobInfo(GridHadoopJobInfo jobInfo) {
        this.jobInfo = jobInfo;
    }

    /**
     * @return Tasks.
     */
    public Collection<GridHadoopTaskInfo> tasks() {
        return tasks;
    }

    /**
     * @param tasks Tasks.
     */
    public void tasks(Collection<GridHadoopTaskInfo> tasks) {
        this.tasks = tasks;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridHadoopTaskExecutionRequest.class, this);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        jobId.writeExternal(out);

        out.writeObject(jobInfo);
        U.writeCollection(out, tasks);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        jobId = new GridHadoopJobId();
        jobId.readExternal(in);

        jobInfo = (GridHadoopJobInfo)in.readObject();
        tasks = U.readCollection(in);
    }
}
