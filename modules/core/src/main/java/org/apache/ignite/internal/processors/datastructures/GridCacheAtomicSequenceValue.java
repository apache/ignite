/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Sequence value.
 */
public final class GridCacheAtomicSequenceValue extends AtomicDataStructureValue implements Cloneable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Counter. */
    @GridToStringInclude(sensitive = true)
    private long val;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridCacheAtomicSequenceValue() {
        // No-op.
    }

    /**
     * Default constructor.
     *
     * @param val Initial value.
     */
    public GridCacheAtomicSequenceValue(long val) {
        this.val = val;
    }

    /** {@inheritDoc} */
    @Override public DataStructureType type() {
        return DataStructureType.ATOMIC_SEQ;
    }

    /**
     * @param val New value.
     */
    public void set(long val) {
        this.val = val;
    }

    /**
     * @return val Current value.
     */
    public long get() {
        return val;
    }

    /** {@inheritDoc} */
    @Override public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(val);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException {
        val = in.readLong();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheAtomicSequenceValue.class, this);
    }
}