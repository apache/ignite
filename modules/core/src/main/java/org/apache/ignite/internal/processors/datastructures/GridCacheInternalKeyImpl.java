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
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Key is used for caching cache data structures.
 */
public class GridCacheInternalKeyImpl implements GridCacheInternalKey, Externalizable, Cloneable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Name of cache data structure. */
    private String name;

    /**
     * Default constructor.
     *
     * @param name - Name of cache data structure.
     */
    public GridCacheInternalKeyImpl(String name) {
        assert !F.isEmpty(name);

        this.name = name;
    }

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridCacheInternalKeyImpl() {
        // No-op.
    }

    /** {@inheritDoc} */
    @AffinityKeyMapped
    @Override public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return name.hashCode();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        return this == obj || (obj instanceof GridCacheInternalKey && name.equals(((GridCacheInternalKey)obj).name()));
    }

    /** {@inheritDoc} */
    @Override public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeString(out, name);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException {
        name = U.readString(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheInternalKeyImpl.class, this);
    }
}