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

package org.apache.ignite.internal.processors.hadoop.shuffle;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.processors.hadoop.HadoopJobId;
import org.apache.ignite.internal.processors.hadoop.message.HadoopMessage;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Acknowledgement message.
 */
public class HadoopShuffleAck implements HadoopMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @GridToStringInclude
    private long msgId;

    /** */
    @GridToStringInclude
    private HadoopJobId jobId;

    /**
     *
     */
    public HadoopShuffleAck() {
        // No-op.
    }

    /**
     * @param msgId Message ID.
     */
    public HadoopShuffleAck(long msgId, HadoopJobId jobId) {
        assert jobId != null;

        this.msgId = msgId;
        this.jobId = jobId;
    }

    /**
     * @return Message ID.
     */
    public long id() {
        return msgId;
    }

    /**
     * @return Job ID.
     */
    public HadoopJobId jobId() {
        return jobId;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        jobId.writeExternal(out);
        out.writeLong(msgId);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        jobId = new HadoopJobId();

        jobId.readExternal(in);
        msgId = in.readLong();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(HadoopShuffleAck.class, this);
    }
}