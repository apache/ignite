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

package org.apache.ignite.internal.processors.cache;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;

/**
 *
 */
public class CacheEntryImplEx<K, V> extends CacheEntryImpl<K, V> implements CacheEntry<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Version. */
    private GridCacheVersion ver;

    /**
     * Required by {@link Externalizable}.
     */
    public CacheEntryImplEx() {
        // No-op.
    }

    /**
     * @param key Key.
     * @param val Value (always null).
     * @param ver Version.
     */
    public CacheEntryImplEx(K key, V val, GridCacheVersion ver) {
        super(key, val);

        this.ver = ver;
    }

    /** {@inheritDoc} */
    public GridCacheVersion version() {
        return ver;
    }

    /** {@inheritDoc} */
    @Override public long updateTime() {
        return ver.globalTime();
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        out.writeObject(ver);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        ver = (GridCacheVersion)in.readObject();
    }

    /** {@inheritDoc} */
    public String toString() {
        return "VersionedEntry [key=" + getKey() + ", val=" + getValue() + ", topVer=" + ver.topologyVersion() +
            ", nodeOrder=" + ver.nodeOrder() + ", order=" + ver.order() + ", updateTime=" + ver.globalTime() + ']';
    }
}