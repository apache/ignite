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

public class GridCacheLockState2Fair extends GridCacheLockState2Base<NodeThread> {
    /** */
    private static final long serialVersionUID = 6727594514711280291L;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridCacheLockState2Fair() {
        super();
    }

    /** {@inheritDoc} */
    @Override public NodeThread removeAll(UUID id) {
        removeNode();

        if (nodes == null || nodes.isEmpty())
            return null;

        final boolean lockReleased = nodes.getFirst().nodeId.equals(id);

        Iterator<NodeThread> iter = nodes.iterator();

        NodeThread result = null;

        while (iter.hasNext()) {
            NodeThread tuple = iter.next();

            if (tuple.nodeId.equals(id)) {
                nodesSet.remove(tuple);
                iter.remove();
            } else if (lockReleased && result == null)
                result = tuple;
        }

        return result;
    }

    /**
     * Constructor.
     *
     * @param gridStartTime Cluster start time.
     */
    public GridCacheLockState2Fair(long gridStartTime) {
        super(gridStartTime);
    }

    /** Clone constructor. */
    protected GridCacheLockState2Fair(GridCacheLockState2Base<NodeThread> state) {
        super(state);
    }

    /** {@inheritDoc} */
    @Override protected Object clone(){
        return new GridCacheLockState2Fair(this);
    }

    /** {@inheritDoc} */
    @Override public void writeItem(ObjectOutput out, NodeThread item) throws IOException {
        out.writeLong(item.nodeId.getMostSignificantBits());
        out.writeLong(item.nodeId.getLeastSignificantBits());
        out.writeLong(item.threadId);
    }

    /** {@inheritDoc} */
    @Override public NodeThread readItem(ObjectInput in) throws IOException {
        UUID id = new UUID(in.readLong(), in.readLong());

        return new NodeThread(id, in.readLong());
    }
}
