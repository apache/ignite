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

/**
 *
 */
public class IpcSharedMemoryInitRequest implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private int pid;

    /**
     * @param pid PID of the {@code client} party.
     */
    public IpcSharedMemoryInitRequest(int pid) {
        this.pid = pid;
    }

    /**
     * Required by {@code Externalizable}.
     */
    public IpcSharedMemoryInitRequest() {
        // No-op.
    }

    /**
     * @return Sender PID.
     */
    public int pid() {
        return pid;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(pid);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        pid = in.readInt();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "IpcSharedMemoryInitRequest [pid=" + pid + ']';
    }
}