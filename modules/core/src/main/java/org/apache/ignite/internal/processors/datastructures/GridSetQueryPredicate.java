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
import org.apache.ignite.IgniteSet;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Predicate for query over {@link IgniteSet} items.
 */
public class GridSetQueryPredicate<K, V> implements IgniteBiPredicate<K, V>, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private IgniteUuid setId;

    /** */
    private boolean collocated;

    /** */
    private GridCacheContext ctx;

    /** */
    private boolean filter;

    /**
     * Required by {@link Externalizable}.
     */
    public GridSetQueryPredicate() {
        // No-op.
    }

    /**
     * @param setId Set ID.
     * @param collocated Collocation flag.
     */
    public GridSetQueryPredicate(IgniteUuid setId, boolean collocated) {
        this.setId = setId;
        this.collocated = collocated;
    }

    /**
     * @param ctx Cache context.
     */
    public void init(GridCacheContext ctx) {
        this.ctx = ctx;

        filter = filterKeys();
    }

    /**
     *
     * @return Collocation flag.
     */
    public boolean collocated() {
        return collocated;
    }

    /**
     * @return Set ID.
     */
    public IgniteUuid setId() {
        return setId;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public boolean apply(K k, V v) {
        return !filter || ctx.affinity().primaryByKey(ctx.localNode(), k, ctx.affinity().affinityTopologyVersion());
    }

    /**
     * @return {@code True} if need to filter out non-primary keys during processing of set data query.
     */
    private boolean filterKeys() {
        return !collocated && !(ctx.isLocal() || ctx.isReplicated()) &&
            (ctx.config().getBackups() > 0 || CU.isNearEnabled(ctx));
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeGridUuid(out, setId);
        out.writeBoolean(collocated);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        setId = U.readGridUuid(in);
        collocated = in.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridSetQueryPredicate.class, this);
    }
}