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

package org.apache.ignite.internal.util.ipc.shmem;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class IpcSharedMemoryInitResponse implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private String inTokFileName;

    /** */
    private int inSharedMemId;

    /** */
    private String outTokFileName;

    /** */
    private int outSharedMemId;

    /** */
    private int pid;

    /** */
    private int size;

    /** */
    private Exception err;

    /**
     * Constructs a successful response.
     *
     * @param inTokFileName In token.
     * @param inSharedMemId In  shared memory ID.
     * @param outTokFileName Out token.
     * @param outSharedMemId Out shared memory ID.
     * @param pid PID of the {@code server} party.
     * @param size Size.
     */
    public IpcSharedMemoryInitResponse(String inTokFileName, int inSharedMemId, String outTokFileName,
                                       int outSharedMemId, int pid, int size) {
        this.inTokFileName = inTokFileName;
        this.inSharedMemId = inSharedMemId;
        this.outTokFileName = outTokFileName;
        this.outSharedMemId = outSharedMemId;
        this.pid = pid;
        this.size = size;
    }

    /**
     * Constructs an error response.
     *
     * @param err Error cause.
     */
    public IpcSharedMemoryInitResponse(Exception err) {
        this.err = err;
    }

    /**
     * Required by {@code Externalizable}.
     */
    public IpcSharedMemoryInitResponse() {
        // No-op.
    }

    /**
     * @return In token file name or {@code null}, if this is an error response.
     */
    @Nullable public String inTokenFileName() {
        return inTokFileName;
    }

    /**
     * @return In shared memory ID.
     */
    public int inSharedMemoryId() {
        return inSharedMemId;
    }

    /**
     * @return Out token file name or {@code null}, if this is an error response.
     */
    @Nullable public String outTokenFileName() {
        return outTokFileName;
    }

    /**
     * @return Out shared memory ID.
     */
    public int outSharedMemoryId() {
        return outSharedMemId;
    }

    /**
     * @return Sender PID.
     */
    public int pid() {
        return pid;
    }

    /**
     * @return Space size.
     */
    public int size() {
        return size;
    }

    /**
     * @return Error message or {@code null}, if this is
     *         a successful response.
     */
    @Nullable public Exception error() {
        return err;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeString(out, inTokFileName);
        out.writeInt(inSharedMemId);
        U.writeString(out, outTokFileName);
        out.writeInt(outSharedMemId);
        out.writeObject(err);
        out.writeInt(pid);
        out.writeInt(size);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        inTokFileName = U.readString(in);
        inSharedMemId = in.readInt();
        outTokFileName = U.readString(in);
        outSharedMemId = in.readInt();
        err = (Exception)in.readObject();
        pid = in.readInt();
        size = in.readInt();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "IpcSharedMemoryInitResponse [err=" + err +
            ", inTokFileName=" + inTokFileName +
            ", inSharedMemId=" + inSharedMemId +
            ", outTokFileName=" + outTokFileName +
            ", outSharedMemId=" + outSharedMemId +
            ", pid=" + pid +
            ", size=" + size + ']';
    }
}