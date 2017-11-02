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
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.jetbrains.annotations.Nullable;

/** */
public class RemoveProcessor<T> implements EntryProcessor<GridCacheInternalKey, GridCacheLockState2Base<T>,
    RemoveProcessor.Tuple<T>>, Externalizable {

    /** */
    private static final long serialVersionUID = 6727594514511280293L;

    /** */
    UUID nodeId;

    /**
     * Empty constructor required for {@link java.io.Externalizable}.
     */
    public RemoveProcessor() {
        // No-op.
    }

    /** */
    public RemoveProcessor(UUID nodeId) {
        assert nodeId != null;

        this.nodeId = nodeId;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Tuple<T> process(MutableEntry<GridCacheInternalKey, GridCacheLockState2Base<T>> entry,
        Object... objects) throws EntryProcessorException {

        assert entry != null;

        if (entry.exists()) {
            GridCacheLockState2Base<T> state = entry.getValue();

            T nextOwner = state.removeAll(nodeId);

            // Always update value in right using.
            entry.setValue(state);

            return new Tuple<>(nextOwner, state.darc <= 0);
        }/* else {
            System.out.println("!!!~ куда-то съебалась запись");
        }*/

        return new Tuple<>(null, true);
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

    /** */
    public static class Tuple<T> {
        /** */
        boolean canRemove;

        /** */
        T owner;

        /** *
         *
         * @param owner
         * @param canRemove
         */
        public Tuple(@Nullable T owner, boolean canRemove){
            this.owner = owner;
            this.canRemove = canRemove;
        }
    }
}
