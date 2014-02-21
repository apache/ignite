// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.apache.commons.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.future.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * {@link GridEvents} implementation.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridEventsImpl implements GridEvents {
    /** */
    private final GridKernalContext ctx;

    /** */
    private final GridProjection prj;

    /**
     * @param ctx Kernal context.
     * @param prj Projection.
     */
    public GridEventsImpl(GridKernalContext ctx, GridProjection prj) {
        this.ctx = ctx;
        this.prj = prj;
    }

    /** {@inheritDoc} */
    @Override public GridProjection projection() {
        return prj;
    }

    /** {@inheritDoc} */
    @Override public GridFuture<List<GridEvent>> queryRemote(GridPredicate<GridEvent> pe, long timeout) {
        A.notNull(pe, "pe");

        guard();

        try {
            return ctx.event().remoteEventsAsync(pe, prj.nodes(), timeout);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <T extends GridEvent> GridFuture<UUID> consumeRemote(@Nullable GridBiPredicate<UUID, T> cb,
        @Nullable GridPredicate<T> filter, @Nullable int... types) {
        return consumeRemote(1, 0, true, cb, filter, types);
    }

    /** {@inheritDoc} */
    @Override public <T extends GridEvent> GridFuture<UUID> consumeRemote(int bufSize, long interval,
        boolean autoUnsubscribe, @Nullable GridBiPredicate<UUID, T> cb, @Nullable GridPredicate<T> filter,
        @Nullable int... types) {
        A.ensure(bufSize > 0, "bufSize > 0");
        A.ensure(interval >= 0, "interval >= 0");

        guard();

        try {
            return ctx.continuous().startRoutine(new GridEventConsumeHandler((GridBiPredicate<UUID, GridEvent>)cb,
                (GridPredicate<GridEvent>)filter, types), bufSize, interval, autoUnsubscribe, prj.predicate());
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public GridFuture<?> stopConsume(@Nullable UUID consumeId) {
        if (consumeId != null) {
            guard();

            try {
                return ctx.continuous().stopRoutine(consumeId);
            }
            finally {
                unguard();
            }
        }
        else
            return new GridFinishedFuture<>(ctx);
    }

    /** {@inheritDoc} */
    @Override public GridFuture<GridEvent> waitForLocal(@Nullable GridPredicate<GridEvent> p,
        @Nullable int... types) {
        guard();

        try {
            return ctx.event().waitForEvent(p, types);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<GridEvent> queryLocal(@Nullable GridPredicate<GridEvent>... p) {
        if (F.isEmpty(p))
            return Collections.emptyList();

        guard();

        try {
            return ctx.event().localEvents(p);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public void recordLocal(GridEvent evt) {
        A.notNull(evt, "evt");

        if (evt.type() <= 1000)
            throw new IllegalArgumentException("All types in range from 1 to 1000 are reserved for " +
                "internal GridGain events [evtType=" + evt.type() + ", evt=" + evt + ']');

        guard();

        try {
            ctx.event().record(evt);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public void addLocalListener(GridLocalEventListener lsnr, int[] types) {
        A.notNull(lsnr, "lsnr");
        A.notNull(types, "types");

        guard();

        try {
            if (types.length == 0)
                throw new GridRuntimeException("Array of event types cannot be empty.");

            ctx.event().addLocalEventListener(lsnr, types);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public void addLocalListener(GridLocalEventListener lsnr, int type, @Nullable int... types) {
        A.notNull(lsnr, "lsnr");

        guard();

        try {
            addLocalListener(lsnr, new int[]{type});

            if (types != null && types.length > 0)
                addLocalListener(lsnr, types);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean removeLocalListener(GridLocalEventListener lsnr, @Nullable int... types) {
        A.notNull(lsnr, "lsnr");

        guard();

        try {
            return ctx.event().removeLocalEventListener(lsnr, types);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public void enableLocal(int[] types) {
        A.notNull(types, "types");

        guard();

        try {
            ctx.event().enableEvents(types);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public void enableLocal(int type, @Nullable int... types) {
        guard();

        try {
            ctx.event().enableEvents(!F.isEmpty(types) ? ArrayUtils.add(types, type) : new int[] {type});
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public void disableLocal(int[] types) {
        A.notNull(types, "types");

        guard();

        try {
            ctx.event().disableEvents(types);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public void disableLocal(int type, @Nullable int... types) {
        guard();

        try {
            ctx.event().disableEvents(!F.isEmpty(types) ? ArrayUtils.add(types, type) : new int[] {type});
        }
        finally {
            unguard();
        }
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
