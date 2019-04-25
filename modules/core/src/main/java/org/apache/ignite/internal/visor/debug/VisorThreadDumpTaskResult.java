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

package org.apache.ignite.internal.visor.debug;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.management.ThreadInfo;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * Data transfer object for Visor {@link ThreadInfo}.
 */
public class VisorThreadDumpTaskResult extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** List of information about threads. */
    private VisorThreadInfo[] threadInfo;

    /** List of deadlocked thread ids. */
    private long[] deadlockedThreads;

    /**
     * Default constructor.
     */
    public VisorThreadDumpTaskResult() {
        // No-op.
    }

    /**
     * Create data transfer object for given thread info.
     *
     * @param threadInfo Thread info.
     * @param deadlockedThreads Thread info.
     */
    public VisorThreadDumpTaskResult(VisorThreadInfo[] threadInfo, long[] deadlockedThreads) {
        this.threadInfo = threadInfo;
        this.deadlockedThreads = deadlockedThreads;
    }

    /**
     * @return List of information about threads.
     */
    public VisorThreadInfo[] getThreadInfo() {
        return threadInfo;
    }

    /**
     * @return List of deadlocked thread ids.
     */
    public long[] getDeadlockedThreads() {
        return deadlockedThreads;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeObject(threadInfo);
        out.writeObject(deadlockedThreads);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        threadInfo = (VisorThreadInfo[])in.readObject();
        deadlockedThreads = (long[])in.readObject();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorThreadDumpTaskResult.class, this);
    }
}
