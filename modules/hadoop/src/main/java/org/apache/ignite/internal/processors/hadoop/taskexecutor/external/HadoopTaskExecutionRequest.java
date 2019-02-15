/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.hadoop.taskexecutor.external;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import org.apache.ignite.internal.processors.hadoop.HadoopJobId;
import org.apache.ignite.internal.processors.hadoop.HadoopJobInfo;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskInfo;
import org.apache.ignite.internal.processors.hadoop.message.HadoopMessage;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Message sent from node to child process to start task(s) execution.
 */
public class HadoopTaskExecutionRequest implements HadoopMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Job ID. */
    @GridToStringInclude
    private HadoopJobId jobId;

    /** Job info. */
    @GridToStringInclude
    private HadoopJobInfo jobInfo;

    /** Mappers. */
    @GridToStringInclude
    private Collection<HadoopTaskInfo> tasks;

    /**
     * @return Job ID.
     */
    public HadoopJobId jobId() {
        return jobId;
    }

    /**
     * @param jobId Job ID.
     */
    public void jobId(HadoopJobId jobId) {
        this.jobId = jobId;
    }

    /**
     * @return Jon info.
     */
    public HadoopJobInfo jobInfo() {
        return jobInfo;
    }

    /**
     * @param jobInfo Job info.
     */
    public void jobInfo(HadoopJobInfo jobInfo) {
        this.jobInfo = jobInfo;
    }

    /**
     * @return Tasks.
     */
    public Collection<HadoopTaskInfo> tasks() {
        return tasks;
    }

    /**
     * @param tasks Tasks.
     */
    public void tasks(Collection<HadoopTaskInfo> tasks) {
        this.tasks = tasks;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(HadoopTaskExecutionRequest.class, this);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        jobId.writeExternal(out);

        out.writeObject(jobInfo);
        U.writeCollection(out, tasks);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        jobId = new HadoopJobId();
        jobId.readExternal(in);

        jobInfo = (HadoopJobInfo)in.readObject();
        tasks = U.readCollection(in);
    }
}