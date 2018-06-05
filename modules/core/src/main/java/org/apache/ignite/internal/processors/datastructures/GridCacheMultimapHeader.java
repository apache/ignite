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
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Multimap header.
 */
public class GridCacheMultimapHeader implements GridCacheInternal, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private IgniteUuid id;

    /** */
    private String multimapName;

    /** */
    private String internalCacheName;

    /** */
    private boolean collocated;

    /** */
    private long size;

    /**
     * Required by {@link Externalizable}.
     */
    public GridCacheMultimapHeader() {
        // No-op.
    }

    /**
     * @param id Multimap unique ID.
     * @param collocated Collocation flag.
     */
    public GridCacheMultimapHeader(IgniteUuid id, String multimapName, String internalCacheName, boolean collocated,
        long size) {
        assert id != null;
        assert multimapName != null;
        assert internalCacheName != null;

        this.id = id;
        this.multimapName = multimapName;
        this.internalCacheName = internalCacheName;
        this.collocated = collocated;
        this.size = size;
    }

    /**
     * @return Multimap unique ID.
     */
    public IgniteUuid id() {
        return id;
    }

    /**
     * @return Multimap name.
     */
    public String multimapName() {
        return multimapName;
    }

    /**
     * @return Internal cache name.
     */
    public String internalCacheName() {
        return internalCacheName;
    }

    /**
     * @return Multimap collocation flag.
     */
    public boolean collocated() {
        return collocated;
    }

    /**
     * @return Multimap size.
     */
    public long size() {
        return size;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeGridUuid(out, id);
        U.writeString(out, multimapName);
        U.writeString(out, internalCacheName);
        out.writeBoolean(collocated);
        out.writeLong(size);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        id = U.readGridUuid(in);
        multimapName = U.readString(in);
        internalCacheName = U.readString(in);
        collocated = in.readBoolean();
        size = in.readLong();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheMultimapHeader.class, this);
    }
}
