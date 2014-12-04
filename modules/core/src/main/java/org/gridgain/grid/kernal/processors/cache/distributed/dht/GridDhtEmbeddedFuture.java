/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.dht;

import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.future.*;
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
