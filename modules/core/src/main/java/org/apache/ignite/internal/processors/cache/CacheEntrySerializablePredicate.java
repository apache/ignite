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

import org.apache.ignite.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.plugin.extensions.communication.*;

import java.nio.*;

/**
 *
 */
public class CacheEntrySerializablePredicate implements CacheEntryPredicate {
    /** */
    @GridToStringInclude
    @GridDirectTransient
    private CacheEntryPredicate[] p;

    /** */
    private byte[] bytes;

    /**
     *
     */
    public CacheEntrySerializablePredicate() {
        // No-op.
    }

    /**
     * @param p Predicate.
     */
    public CacheEntrySerializablePredicate(CacheEntryPredicate... p) {
        assert p != null;

        this.p = p;
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        assert bytes != null;

        p = ctx.marshaller().unmarshal(bytes, ldr);
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheContext ctx) throws IgniteCheckedException {
        assert p != null;

        bytes = ctx.marshaller().marshal(p);
    }

    /** {@inheritDoc} */
    @Override public boolean apply(GridCacheEntryEx e) {
        assert p != null;

        for (CacheEntryPredicate p0 : p) {
            if (!p0.apply(e))
                return false;
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 0;
    }
}
