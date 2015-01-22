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

package org.gridgain.grid.kernal.processors.cache.distributed.dht;

import org.apache.ignite.lang.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.apache.ignite.internal.util.future.*;
import org.gridgain.grid.util.tostring.*;

import java.io.*;
import java.util.*;

/**
 * Embedded DHT future.
 */
public class GridDhtEmbeddedFuture<A, B> extends GridEmbeddedFuture<A, B> implements GridDhtFuture<A> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Retries. */
    @GridToStringInclude
    private Collection<Integer> invalidParts;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridDhtEmbeddedFuture() {
        // No-op.
    }

    /**
     * @param ctx Context.
     * @param embedded Embedded.
     * @param c Closure.
     */
    public GridDhtEmbeddedFuture(GridKernalContext ctx, IgniteFuture<B> embedded, IgniteBiClosure<B, Exception, A> c) {
        super(ctx, embedded, c);

        invalidParts = Collections.emptyList();
    }

    /**
     * @param embedded Future to embed.
     * @param c Embedding closure.
     * @param ctx Kernal context.
     */
    public GridDhtEmbeddedFuture(IgniteFuture<B> embedded,
        IgniteBiClosure<B, Exception, IgniteFuture<A>> c, GridKernalContext ctx) {
        super(embedded, c, ctx);

        invalidParts = Collections.emptyList();
    }

    /**
     * @param ctx Context.
     * @param embedded Embedded.
     * @param c Closure.
     * @param invalidParts Retries.
     */
    public GridDhtEmbeddedFuture(GridKernalContext ctx, IgniteFuture<B> embedded, IgniteBiClosure<B, Exception, A> c,
        Collection<Integer> invalidParts) {
        super(ctx, embedded, c);

        this.invalidParts = invalidParts;
    }

    /** {@inheritDoc} */
    @Override public Collection<Integer> invalidPartitions() {
        return invalidParts;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtEmbeddedFuture.class, this, super.toString());
    }
}
