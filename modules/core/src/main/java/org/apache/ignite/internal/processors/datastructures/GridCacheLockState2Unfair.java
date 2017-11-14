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

/** Unfair implementation of shared lock state. */
public final class GridCacheLockState2Unfair extends GridCacheLockState2Base<UUID> {
    /** */
    private static final long serialVersionUID = 6727594514511280291L;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridCacheLockState2Unfair() {
        super();
    }

    /**
     * Constructor.
     *
     * @param gridStartTime Cluster start time.
     */
    public GridCacheLockState2Unfair(long gridStartTime) {
        super(gridStartTime);
    }

    /** {@inheritDoc} */
    @Override public UUID onNodeRemoved(UUID id) {
        if (nodesSet.remove(id)) {
            final boolean lockReleased = nodes.getFirst().equals(id);

            nodes.remove(id);

            if (lockReleased && !nodes.isEmpty())
                return nodes.getFirst();
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public void writeItem(ObjectOutput out, UUID item) throws IOException {
        out.writeLong(item.getMostSignificantBits());
        out.writeLong(item.getLeastSignificantBits());
    }

    /** {@inheritDoc} */
    @Override public UUID readItem(ObjectInput in) throws IOException {
        return new UUID(in.readLong(), in.readLong());
    }
}
