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

package org.apache.ignite.internal.visor.cache;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * Arguments for {@link VisorCacheLoadTask}.
 */
public class VisorCacheLoadTaskArg extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cache names to load data. */
    private Set<String> cacheNames;

    /** Duration a Cache Entry should exist be before it expires after being modified. */
    private long ttl;

    /** Optional user arguments to be passed into CacheStore.loadCache(IgniteBiInClosure, Object...) method. */
    private Object[] ldrArgs;

    /**
     * Default constructor.
     */
    public VisorCacheLoadTaskArg() {
        // No-op.
    }

    /**
     * @param cacheNames Cache names to load data.
     * @param ttl Duration a Cache Entry should exist be before it expires after being modified.
     * @param ldrArgs Optional user arguments to be passed into CacheStore.loadCache(IgniteBiInClosure, Object...) method.
     */
    public VisorCacheLoadTaskArg(Set<String> cacheNames, long ttl, Object[] ldrArgs) {
        this.cacheNames = cacheNames;
        this.ttl = ttl;
        this.ldrArgs = ldrArgs;
    }

    /**
     * @return Cache names to load data.
     */
    public Set<String> getCacheNames() {
        return cacheNames;
    }

    /**
     * @return Duration a Cache Entry should exist be before it expires after being modified.
     */
    public long getTtl() {
        return ttl;
    }

    /**
     * @return Optional user arguments to be passed into CacheStore.loadCache(IgniteBiInClosure, Object...) method.
     */
    public Object[] getLoaderArguments() {
        return ldrArgs;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeCollection(out, cacheNames);
        out.writeLong(ttl);
        U.writeArray(out, ldrArgs);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        cacheNames = U.readSet(in);
        ttl = in.readLong();
        ldrArgs = U.readArray(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCacheLoadTaskArg.class, this);
    }
}
