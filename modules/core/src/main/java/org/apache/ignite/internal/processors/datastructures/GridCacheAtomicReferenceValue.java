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
import org.apache.ignite.internal.util.lang.GridPeerDeployAware;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Atomic reference value.
 */
public final class GridCacheAtomicReferenceValue<T> extends AtomicDataStructureValue implements GridPeerDeployAware {
    /** */
    private static final long serialVersionUID = 0L;

    /** Value. */
    private T val;

    /**
     * Default constructor.
     *
     * @param val Initial value.
     */
    public GridCacheAtomicReferenceValue(T val) {
        this.val = val;
    }

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridCacheAtomicReferenceValue() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public DataStructureType type() {
        return DataStructureType.ATOMIC_REF;
    }

    /**
     * @param val New value.
     */
    public void set(T val) {
        this.val = val;
    }

    /**
     * @return val Current value.
     */
    public T get() {
        return val;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(val);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        val = (T)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public Class<?> deployClass() {
        // First of all check classes that may be loaded by class loader other than application one.
        return val != null ? val.getClass() : getClass();
    }

    /** {@inheritDoc} */
    @Override public ClassLoader classLoader() {
        return deployClass().getClassLoader();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheAtomicReferenceValue.class, this);
    }
}