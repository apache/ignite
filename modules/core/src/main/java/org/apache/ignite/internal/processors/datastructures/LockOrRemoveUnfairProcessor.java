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

/** EntryProcessor for release lock by timeout, but acquire it if lock has released. */
public final class LockOrRemoveUnfairProcessor extends ReentrantProcessor<UUID> {
    /** */
    private static final long serialVersionUID = 2968825754944751240L;

    /** */
    private UUID nodeId;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public LockOrRemoveUnfairProcessor() {
        // No-op.
    }

    /** */
    public LockOrRemoveUnfairProcessor(UUID nodeId) {
        assert nodeId != null;

        this.nodeId = nodeId;
    }

    /** {@inheritDoc} */
    @Override protected LockedModified tryLock(GridCacheLockState2Base<UUID> state) {
        return state.lockOrRemove(nodeId);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(nodeId.getMostSignificantBits());
        out.writeLong(nodeId.getLeastSignificantBits());
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        nodeId = new UUID(in.readLong(), in.readLong());
    }
}
