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

package org.gridgain.grid.kernal.processors.cache.datastructures;

import org.apache.ignite.lang.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

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

    /**
     * Required by {@link Externalizable}.
     */
    public GridCacheSetHeader() {
        // No-op.
    }

    /**
     * @param id Set UUID.
     * @param collocated Collocation flag.
     */
    public GridCacheSetHeader(IgniteUuid id, boolean collocated) {
        this.id = id;
        this.collocated = collocated;
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

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeGridUuid(out, id);
        out.writeBoolean(collocated);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        id = U.readGridUuid(in);
        collocated = in.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheSetHeader.class, this);
    }
}
