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
import java.util.List;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;
import org.jetbrains.annotations.Nullable;

/**
 * Data transfer object for configuration of hadoop data structures.
 */
public class VisorHadoopConfiguration extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Map reduce planner. */
    private String planner;

    /** */
    private boolean extExecution;

    /** Finished job info TTL. */
    private long finishedJobInfoTtl;

    /** */
    private int maxParallelTasks;

    /** */
    private int maxTaskQueueSize;

    /** Library names. */
    private List<String> libNames;

    /**
     * Default constructor.
     */
    public VisorHadoopConfiguration() {
        // No-op.
    }

    /**
     * @return Max number of local tasks that may be executed in parallel.
     */
    public int getMaxParallelTasks() {
        return maxParallelTasks;
    }

    /**
     * @return Max task queue size.
     */
    public int getMaxTaskQueueSize() {
        return maxTaskQueueSize;
    }

    /**
     * @return Finished job info time-to-live.
     */
    public long getFinishedJobInfoTtl() {
        return finishedJobInfoTtl;
    }

    /**
     * @return {@code True} if external execution.
     */
    public boolean isExternalExecution() {
        return extExecution;
    }

    /**
     * @return Map-reduce planner.
     */
    public String getMapReducePlanner() {
        return planner;
    }

    /**
     * @return Native library names.
     */
    @Nullable public List<String> getNativeLibraryNames() {
        return libNames;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, planner);
        out.writeBoolean(extExecution);
        out.writeLong(finishedJobInfoTtl);
        out.writeInt(maxParallelTasks);
        out.writeInt(maxTaskQueueSize);
        U.writeCollection(out, libNames);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        planner = U.readString(in);
        extExecution = in.readBoolean();
        finishedJobInfoTtl = in.readLong();
        maxParallelTasks = in.readInt();
        maxTaskQueueSize = in.readInt();
        libNames = U.readList(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorHadoopConfiguration.class, this);
    }
}
