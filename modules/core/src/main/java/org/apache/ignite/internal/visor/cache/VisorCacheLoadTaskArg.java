/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
