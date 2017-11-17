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
import org.jetbrains.annotations.Nullable;

/** {@link CacheEntryProcessor} for a release operation in unfair mode. */
public final class ReleaseUnfairProcessor implements CacheEntryProcessor<GridCacheInternalKey, GridCacheLockState2Base<UUID>, UUID>,
    Externalizable {
    /** */
    private static final long serialVersionUID = 6727594514511280293L;

    /** */
    private UUID nodeId;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public ReleaseUnfairProcessor() {
        // No-op.
    }

    /** */
    public ReleaseUnfairProcessor(UUID nodeId) {
        assert nodeId != null;

        this.nodeId = nodeId;
    }

    /** {@inheritDoc} */
    @Nullable @Override public UUID process(MutableEntry<GridCacheInternalKey, GridCacheLockState2Base<UUID>> entry,
        Object... objects) throws EntryProcessorException {

        assert entry != null;

        if (entry.exists()) {
            GridCacheLockState2Base<UUID> state = entry.getValue();

            UUID nextId = state.unlock(nodeId);

            // Always update value in right using.
            entry.setValue(state);

            return nextId;
        }

        return null;
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
