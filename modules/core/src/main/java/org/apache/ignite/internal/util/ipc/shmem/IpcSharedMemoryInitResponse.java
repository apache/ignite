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