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

import java.io.EOFException;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.processors.cache.GridCacheInternal;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Cache set header.
 */
public class GridCacheSetHeader implements GridCacheInternal, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private IgniteUuid id;

    /** */
    private boolean collocated;

    /** {@code True} If this version of IgniteSet uses separated cache. */
    private boolean separatedCache;

    /**
     * Required by {@link Externalizable}.
     */
    public GridCacheSetHeader() {
        // No-op.
    }

    /**
     * @param id Set UUID.
     * @param collocated Collocation flag.
     * @param separatedCache {@code True} If this version of IgniteSet uses separated cache.
     */
    public GridCacheSetHeader(IgniteUuid id, boolean collocated, boolean separatedCache) {
        assert !(separatedCache && collocated);

        this.id = id;
        this.collocated = collocated;
        this.separatedCache = separatedCache;
    }

    /**
     * @return Set unique ID.
     */
    public IgniteUuid id() {
        return id;
    }

    /**
     * @return Collocation flag.
     */
    public boolean collocated() {
        return collocated;
    }

    /**
     * @return {@code True} If this version uses separated cache.
     */
    public boolean separatedCache() {
        return separatedCache;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeGridUuid(out, id);
        out.writeBoolean(collocated);
        out.writeBoolean(separatedCache);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException {
        try {
            id = U.readGridUuid(in);
            collocated = in.readBoolean();
            separatedCache = in.readBoolean();
        }
        catch (EOFException ignore) {
            // No-op.
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheSetHeader.class, this);
    }
}