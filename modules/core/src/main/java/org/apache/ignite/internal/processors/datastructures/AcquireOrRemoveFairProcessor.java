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

/** {@link org.apache.ignite.cache.CacheEntryProcessor} for a final try acquire and remove if failed for fair mode. */
public final class AcquireOrRemoveFairProcessor extends ReentrantProcessor<LockOwner> {
    /** */
    private static final long serialVersionUID = 2968825754944751240L;

    /** Lock owner. */
    private LockOwner owner;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public AcquireOrRemoveFairProcessor() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param owner Lock owner.
     */
    AcquireOrRemoveFairProcessor(LockOwner owner) {
        assert owner != null;

        this.owner = owner;
    }

    /** {@inheritDoc} */
    @Override protected LockedModified lock(GridCacheLockState2Base<LockOwner> state) {
        assert state != null;

        return state.lockOrRemove(owner);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(owner.nodeId.getMostSignificantBits());
        out.writeLong(owner.nodeId.getLeastSignificantBits());
        out.writeLong(owner.threadId);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException {
        UUID nodeId = new UUID(in.readLong(), in.readLong());

        owner = new LockOwner(nodeId, in.readLong());
    }
}
