// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.continuous.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.messaging.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * {@link GridMessaging} implementation.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridMessagingImpl implements GridMessaging {
    /** */
    private final GridKernalContext ctx;

    /** */
    private final GridProjection prj;

    /**
     * @param ctx Kernal context.
     * @param prj Projection.
     */
    public GridMessagingImpl(GridKernalContext ctx, GridProjection prj) {
        this.ctx = ctx;
        this.prj = prj;
    }

    /** {@inheritDoc} */
    @Override public GridProjection projection() {
        return prj;
    }

    /** {@inheritDoc} */
    @Override public void send(@Nullable Object topic, Object msg) throws GridException {
        A.notNull(msg, "msg");

        guard();

        try {
            Collection<GridNode> snapshot = prj.nodes();

            if (snapshot.isEmpty())
                throw U.emptyTopologyException();

            ctx.io().sendUserMessage(snapshot, msg, topic, false, 0);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public void send(@Nullable Object topic, Collection<?> msgs) throws GridException {
        A.ensure(!F.isEmpty(msgs), "msgs cannot be null or empty");

        guard();

        try {
            Collection<GridNode> snapshot = prj.nodes();

            if (snapshot.isEmpty())
                throw U.emptyTopologyException();

            for (Object msg : msgs) {
                A.notNull(msg, "msg");

                ctx.io().sendUserMessage(snapshot, msg, topic, false, 0);
            }
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public void sendOrdered(@Nullable Object topic, Object msg, long timeout) throws GridException {
        A.notNull(msg, "msg");

        guard();

        try {
            Collection<GridNode> snapshot = prj.nodes();

            if (snapshot.isEmpty())
                throw U.emptyTopologyException();

            ctx.io().sendUserMessage(snapshot, msg, topic, true, timeout);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void localListen(@Nullable Object topic, GridBiPredicate<UUID, ?> p) {
        A.notNull(p, "p");

        guard();

        try {
            ctx.io().listenAsync(topic, p);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public GridFuture<UUID> remoteListen(@Nullable Object topic, GridBiPredicate<UUID, ?> p) {
        A.notNull(p, "p");

        guard();

        try {
            GridContinuousHandler hnd = new GridMessageListenHandler(topic, (GridBiPredicate<UUID, Object>)p);

            return ctx.continuous().startRoutine(hnd, 1, 0, false, prj.predicate());
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public GridFuture<?> stopRemoteListen(UUID opId) {
        A.notNull(opId, "opId");

        return ctx.continuous().stopRoutine(opId);
    }

    /**
     * <tt>ctx.gateway().readLock()</tt>
     */
    private void guard() {
        ctx.gateway().readLock();
    }

    /**
     * <tt>ctx.gateway().readUnlock()</tt>
     */
    private void unguard() {
        ctx.gateway().readUnlock();
    }
}
