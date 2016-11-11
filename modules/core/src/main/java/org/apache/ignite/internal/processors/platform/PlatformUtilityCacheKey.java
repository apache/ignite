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

package org.apache.ignite.internal.processors.platform;

import org.apache.ignite.internal.processors.cache.GridCacheUtils;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Composite cache key for platform utility cache (see {@link GridCacheUtils}).
 */
public class PlatformUtilityCacheKey implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private byte prefix;

    /** */
    private Object id;

    /**
     * Default ctor for serialization.
     */
    public PlatformUtilityCacheKey() {
        // No-op.
    }

    /**
     * Ctor.
     *
     * @param prefix Prefix.
     * @param id Id.
     */
    public PlatformUtilityCacheKey(byte prefix, Object id) {
        assert id != null;

        this.prefix = prefix;
        this.id = id;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeByte(prefix);
        out.writeObject(id);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        prefix = in.readByte();
        id = in.readObject();

        assert id != null;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        PlatformUtilityCacheKey key = (PlatformUtilityCacheKey)o;

        return prefix == key.prefix && id.equals(key.id);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return 31 * (int)prefix + id.hashCode();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return prefix + "_" + id.toString();
    }
}
