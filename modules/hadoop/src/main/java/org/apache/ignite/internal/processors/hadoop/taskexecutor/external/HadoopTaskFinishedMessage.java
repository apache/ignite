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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskInfo;
import org.apache.ignite.internal.processors.hadoop.message.HadoopMessage;
import org.apache.ignite.internal.processors.hadoop.taskexecutor.HadoopTaskStatus;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Task finished message. Sent when local task finishes execution.
 */
public class HadoopTaskFinishedMessage implements HadoopMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Finished task info. */
    private HadoopTaskInfo taskInfo;

    /** Task finish status. */
    private HadoopTaskStatus status;

    /**
     * Constructor required by {@link Externalizable}.
     */
    public HadoopTaskFinishedMessage() {
        // No-op.
    }

    /**
     * @param taskInfo Finished task info.
     * @param status Task finish status.
     */
    public HadoopTaskFinishedMessage(HadoopTaskInfo taskInfo, HadoopTaskStatus status) {
        assert taskInfo != null;
        assert status != null;

        this.taskInfo = taskInfo;
        this.status = status;
    }

    /**
     * @return Finished task info.
     */
    public HadoopTaskInfo taskInfo() {
        return taskInfo;
    }

    /**
     * @return Task finish status.
     */
    public HadoopTaskStatus status() {
        return status;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(HadoopTaskFinishedMessage.class, this);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        taskInfo.writeExternal(out);
        status.writeExternal(out);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        taskInfo = new HadoopTaskInfo();
        taskInfo.readExternal(in);

        status = new HadoopTaskStatus();
        status.readExternal(in);
    }
}