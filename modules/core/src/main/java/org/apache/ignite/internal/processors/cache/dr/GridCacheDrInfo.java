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

package org.apache.ignite.internal.processors.cache.dr;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import javax.cache.processor.EntryProcessor;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Cache DR info used as argument in PUT cache internal interfaces.
 */
public class GridCacheDrInfo implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Value. */
    private CacheObject val;

    /** Entry processor. */
    private EntryProcessor proc;

    /** DR version. */
    private GridCacheVersion ver;

    /**
     * {@link Externalizable} support.
     */
    public GridCacheDrInfo() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param val Value.
     * @param ver Version.
     */
    public GridCacheDrInfo(CacheObject val, GridCacheVersion ver) {
        assert val != null;
        assert ver != null;

        this.val = val;
        this.ver = ver;
    }

    /**
     * Constructor.
     *
     * @param ver Version.
     */
    public GridCacheDrInfo(GridCacheVersion ver) {
        this.ver = ver;
    }

    /**
     * Constructor.
     *
     * @param proc Entry processor.
     * @param ver Version.
     */
    public GridCacheDrInfo(EntryProcessor proc, GridCacheVersion ver) {
        assert proc != null;
        assert ver != null;

        this.proc = proc;
        this.ver = ver;
    }

    /**
     * @return Value.
     */
    public CacheObject value() {
        return val;
    }

    /**
     * @return Entry processor.
     */
    public EntryProcessor entryProcessor() {
        return proc;
    }

    /**
     * @return Value (entry processor or cache object.
     */
    public Object valueEx() {
        return val == null ? proc : val;
    }

    /**
     * @return Version.
     */
    public GridCacheVersion version() {
        return ver;
    }

    /**
     * @return TTL.
     */
    public long ttl() {
        return CU.TTL_ETERNAL;
    }

    /**
     * @return Expire time.
     */
    public long expireTime() {
        return CU.EXPIRE_TIME_ETERNAL;
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        assert false;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        assert false;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheDrInfo.class, this);
    }
}