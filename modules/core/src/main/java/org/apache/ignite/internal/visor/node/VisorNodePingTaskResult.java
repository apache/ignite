/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * Result for {@link VisorNodePingTask}.
 */
public class VisorNodePingTaskResult extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Node alive. */
    private boolean alive;

    /** Ping start time. */
    private long startTime;

    /** Ping finish time. */
    private long finishTime;

    /**
     * Default constructor.
     */
    public VisorNodePingTaskResult() {
        // No-op.
    }

    /**
     * @param alive Node alive.
     * @param startTime Ping start time.
     * @param finishTime Ping finish time.
     */
    public VisorNodePingTaskResult(boolean alive, long startTime, long finishTime) {
        this.alive = alive;
        this.startTime = startTime;
        this.finishTime = finishTime;
    }

    /**
     * @return Node alive.
     */
    public boolean isAlive() {
        return alive;
    }

    /**
     * @return Ping start time.
     */
    public long getStartTime() {
        return startTime;
    }

    /**
     * @return Ping finish time.
     */
    public long getFinishTime() {
        return finishTime;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeBoolean(alive);
        out.writeLong(startTime);
        out.writeLong(finishTime);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        alive = in.readBoolean();
        startTime = in.readLong();
        finishTime = in.readLong();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorNodePingTaskResult.class, this);
    }
}
