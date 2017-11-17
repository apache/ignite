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

package org.apache.ignite.internal.processors.datastructures;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;

/** Simple tuple for a lock owner. */
public final class LockOwner implements Externalizable {
    /** */
    private static final long serialVersionUID = -5203487119206054926L;

    /** */
    UUID nodeId;

    /** */
    long threadId;

    /**
     * Required by {@link Externalizable}.
     */
    public LockOwner() {
        // No-op.
    }

    /** */
    LockOwner(UUID nodeId, long threadId) {
        assert nodeId != null;

        this.nodeId = nodeId;
        this.threadId = threadId;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        LockOwner thread = (LockOwner)o;

        if (threadId != thread.threadId)
            return false;
        return nodeId.equals(thread.nodeId);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int result = nodeId.hashCode();
        result = 31 * result + (int)(threadId ^ (threadId >>> 32));
        return result;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(nodeId.getMostSignificantBits());
        out.writeLong(nodeId.getLeastSignificantBits());
        out.writeLong(threadId);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException {
        nodeId = new UUID(in.readLong(), in.readLong());
        threadId = in.readLong();
    }
}
