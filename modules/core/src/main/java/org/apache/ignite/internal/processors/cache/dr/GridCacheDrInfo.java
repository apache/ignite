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

package org.apache.ignite.internal.processors.cache.dr;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
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
     * @return Value.
     */
    public CacheObject value() {
        return val;
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

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        assert false;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        assert false;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheDrInfo.class, this);
    }
}