/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.apache.ignite.cluster.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * {@link org.gridgain.grid.events.IgniteEvents} implementation.
 */
public class IgniteEventsImpl extends IgniteAsyncSupportAdapter implements IgniteEvents, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private GridKernalContext ctx;

    /** */
    private ClusterGroupAdapter prj;

    /**
     * Required by {@link Externalizable}.
     */
    public IgniteEventsImpl() {
        // No-op.
    }

    /**
     * @param ctx Kernal context.
     * @param prj Projection.
     * @param async Async support flag.
     */
    public IgniteEventsImpl(GridKernalContext ctx, ClusterGroupAdapter prj, boolean async) {
        super(async);

        this.ctx = ctx;
        this.prj = prj;
    }

    /** {@inheritDoc} */
    @Override public ClusterGroup projection() {
        return prj;
    }

    /** {@inheritDoc} */
    @Override public <T extends GridEvent> List<T> remoteQuery(IgnitePredicate<T> p, long timeout,
        @Nullable int... types) throws GridException {
        A.notNull(p, "p");

        guard();

        try {
            return saveOrGet(ctx.event().remoteEventsAsync(compoundPredicate(p, types), prj.nodes(), timeout));
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <T extends GridEvent> UUID remoteListen(@Nullable IgniteBiPredicate<UUID, T> locLsnr,
        @Nullable IgnitePredicate<T> rmtFilter, @Nullable int... types) throws GridException {
        return remoteListen(1, 0, true, locLsnr, rmtFilter, types);
    }

    /** {@inheritDoc} */
    @Override public <T extends GridEvent> UUID remoteListen(int bufSize, long interval,
        boolean autoUnsubscribe, @Nullable IgniteBiPredicate<UUID, T> locLsnr, @Nullable IgnitePredicate<T> rmtFilter,
        @Nullable int... types) throws GridException {
        A.ensure(bufSize > 0, "bufSize > 0");
        A.ensure(interval >= 0, "interval >= 0");

        guard();

        try {
            return saveOrGet(ctx.continuous().startRoutine(
                new GridEventConsumeHandler((IgniteBiPredicate<UUID, GridEvent>)locLsnr,
                    (IgnitePredicate<GridEvent>)rmtFilter, types), bufSize, interval, autoUnsubscribe, prj.predicate()));
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public void stopRemoteListen(UUID opId) throws GridException {
        A.notNull(opId, "consumeId");

        guard();

        try {
            saveOrGet(ctx.continuous().stopRoutine(opId));
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <T extends GridEvent> T waitForLocal(@Nullable IgnitePredicate<T> filter,
        @Nullable int... types) throws GridException {
        guard();

        try {
            return saveOrGet(ctx.event().waitForEvent(filter, types));
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <T extends GridEvent> Collection<T> localQuery(IgnitePredicate<T> p, @Nullable int... types) {
        A.notNull(p, "p");

        guard();

        try {
            return ctx.event().localEvents(compoundPredicate(p, types));
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
    @Override public void localListen(IgnitePredicate<? extends GridEvent> lsnr, int[] types) {
        A.notNull(lsnr, "lsnr");
        A.notEmpty(types, "types");

        guard();

        try {
            ctx.event().addLocalEventListener(lsnr, types);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean stopLocalListen(IgnitePredicate<? extends GridEvent> lsnr, @Nullable int... types) {
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
        A.notEmpty(types, "types");

        guard();

        try {
            ctx.event().enableEvents(types);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public void disableLocal(int[] types) {
        A.notEmpty(types, "types");

        guard();

        try {
            ctx.event().disableEvents(types);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public int[] enabledEvents() {
        return ctx.event().enabledEvents();
    }

    /** {@inheritDoc} */
    @Override public boolean isEnabled(int type) {
        if (type < 0)
            throw new IllegalArgumentException("Invalid event type: " + type);

        return ctx.event().isUserRecordable(type);
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

    /**
     * @param p Predicate.
     * @param types Event types.
     * @return Compound predicate.
     */
    private static <T extends GridEvent> IgnitePredicate<T> compoundPredicate(final IgnitePredicate<T> p,
        @Nullable final int... types) {

        return F.isEmpty(types) ? p :
            new IgnitePredicate<T>() {
                @Override public boolean apply(T t) {
                    for (int type : types) {
                        if (type == t.type())
                            return p.apply(t);
                    }

                    return false;
                }
            };
    }

    /** {@inheritDoc} */
    @Override public IgniteEvents enableAsync() {
        if (isAsync())
            return this;

        return new IgniteEventsImpl(ctx, prj, true);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(prj);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        prj = (ClusterGroupAdapter)in.readObject();
    }

    /**
     * Reconstructs object on unmarshalling.
     *
     * @return Reconstructed object.
     * @throws ObjectStreamException Thrown in case of unmarshalling error.
     */
    protected Object readResolve() throws ObjectStreamException {
        return prj.events();
    }
}
