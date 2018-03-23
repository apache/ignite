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
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.cache.CacheEntryProcessor;

/** {@link CacheEntryProcessor} for a closing reentrant lock state. */
public final class CloseProcessor<T>
    implements CacheEntryProcessor<GridCacheInternalKey, GridCacheLockState2Base<T>, Boolean>, Externalizable {
    /** */
    private static final long serialVersionUID = 6727594514512370293L;

    /** Removed node ID. */
    private UUID nodeId;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public CloseProcessor() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param nodeId Node id.
     */
    public CloseProcessor(UUID nodeId) {
        this.nodeId = nodeId;
    }

    /** {@inheritDoc} */
    @Override public Boolean process(MutableEntry<GridCacheInternalKey, GridCacheLockState2Base<T>> entry,
        Object... arguments) throws EntryProcessorException {
        assert entry.exists();

        GridCacheLockState2Base<T> state = entry.getValue();

        assert state != null;

        assert state.checkConsistency();

        state.removeNode(nodeId);

        assert state.checkConsistency();

        if (state.canRemove()) {
            entry.remove();

            return true;
        }
        else {
            entry.setValue(state);

            return false;
        }
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(nodeId.getMostSignificantBits());
        out.writeLong(nodeId.getLeastSignificantBits());
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException {
        nodeId = new UUID(in.readLong(), in.readLong());
    }
}
