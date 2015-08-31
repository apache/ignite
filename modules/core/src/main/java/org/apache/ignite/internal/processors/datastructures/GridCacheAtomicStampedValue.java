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
import org.apache.ignite.internal.processors.cache.GridCacheInternal;
import org.apache.ignite.internal.util.lang.GridPeerDeployAware;
import org.apache.ignite.internal.util.tostring.GridToStringBuilder;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;

/**
 * Atomic stamped value.
 */

public final class GridCacheAtomicStampedValue<T, S> implements GridCacheInternal, GridPeerDeployAware,
    Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Value. */
    private T val;

    /** Stamp. */
    private S stamp;

    /**
     * Constructor.
     *
     * @param val Initial value.
     * @param stamp Initial stamp.
     */
    public GridCacheAtomicStampedValue(T val, S stamp) {
        this.val = val;
        this.stamp = stamp;
    }

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridCacheAtomicStampedValue() {
        // No-op.
    }

    /**
     * @param val New value.
     * @param stamp New stamp.
     */
    public void set(T val, S stamp) {
        this.val = val;
        this.stamp = stamp;
    }

    /**
     * @return Current value and stamp.
     */
    public IgniteBiTuple<T, S> get() {
        return F.t(val, stamp);
    }

    /**
     * @return val Current value.
     */
    public T value() {
        return val;
    }

    /**
     * @return Current stamp.
     */
    public S stamp() {
        return stamp;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(val);
        out.writeObject(stamp);
    }

    /** {@inheritDoc} */
    @SuppressWarnings( {"unchecked"})
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        val = (T)in.readObject();
        stamp = (S)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public Class<?> deployClass() {
        ClassLoader clsLdr = getClass().getClassLoader();

        // First of all check classes that may be loaded by class loader other than application one.
        return stamp != null && !clsLdr.equals(stamp.getClass().getClassLoader()) ?
            stamp.getClass() : val != null ? val.getClass() : getClass();
    }

    /** {@inheritDoc} */
    @Override public ClassLoader classLoader() {
        return deployClass().getClassLoader();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return GridToStringBuilder.toString(GridCacheAtomicStampedValue.class, this);
    }
}