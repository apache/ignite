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
import java.util.Iterator;
import java.util.UUID;
import org.jetbrains.annotations.Nullable;

/** The fair implementation for shared lock state. */
public final class GridCacheLockState2Fair extends GridCacheLockState2Base<LockOwner> {
    /** */
    private static final long serialVersionUID = 6727594514711280291L;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridCacheLockState2Fair() {
        super();
    }

    /**
     * Constructor.
     *
     * @param gridStartTime Cluster start time.
     */
    GridCacheLockState2Fair(long gridStartTime) {
        super(gridStartTime);
    }

    /** {@inheritDoc} */
    @Nullable @Override LockOwner onNodeRemoved(UUID id) {
        if (owners == null || owners.isEmpty())
            return null;

        boolean lockReleased = owners.getFirst().nodeId.equals(id);

        Iterator<LockOwner> iter = owners.iterator();

        LockOwner nextOwner = null;

        while (iter.hasNext()) {
            LockOwner tuple = iter.next();

            if (tuple.nodeId.equals(id)) {
                ownerSet.remove(tuple);
                iter.remove();
            }
            else if (lockReleased && nextOwner == null)
                nextOwner = tuple;
        }

        return nextOwner;
    }

    /** {@inheritDoc} */
    @Override public void writeItem(ObjectOutput out, LockOwner item) throws IOException {
        out.writeLong(item.nodeId.getMostSignificantBits());
        out.writeLong(item.nodeId.getLeastSignificantBits());
        out.writeLong(item.threadId);
    }

    /** {@inheritDoc} */
    @Override public LockOwner readItem(ObjectInput in) throws IOException {
        UUID id = new UUID(in.readLong(), in.readLong());

        return new LockOwner(id, in.readLong());
    }
}
