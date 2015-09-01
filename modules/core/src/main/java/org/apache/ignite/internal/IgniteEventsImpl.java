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

package org.apache.ignite.internal;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.ObjectStreamException;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteEvents;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.cluster.ClusterGroupAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgnitePredicate;
import org.jetbrains.annotations.Nullable;

/**
 * {@link IgniteEvents} implementation.
 */
public class IgniteEventsImpl extends AsyncSupportAdapter<IgniteEvents> implements IgniteEvents, Externalizable {
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
    @Override public ClusterGroup clusterGroup() {
        return prj;
    }

    /** {@inheritDoc} */
    @Override public <T extends Event> List<T> remoteQuery(IgnitePredicate<T> p, long timeout,
        @Nullable int... types) {
        A.notNull(p, "p");

        guard();

        try {
            return saveOrGet(ctx.event().remoteEventsAsync(compoundPredicate(p, types), prj.nodes(), timeout));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <T extends Event> UUID remoteListen(@Nullable IgniteBiPredicate<UUID, T> locLsnr,
        @Nullable IgnitePredicate<T> rmtFilter, @Nullable int... types) {
        return remoteListen(1, 0, true, locLsnr, rmtFilter, types);
    }

    /** {@inheritDoc} */
    @Override public <T extends Event> UUID remoteListen(int bufSize, long interval,
        boolean autoUnsubscribe, @Nullable IgniteBiPredicate<UUID, T> locLsnr, @Nullable IgnitePredicate<T> rmtFilter,
        @Nullable int... types) {
        A.ensure(bufSize > 0, "bufSize > 0");
        A.ensure(interval >= 0, "interval >= 0");

        guard();

        try {
            return saveOrGet(ctx.continuous().startRoutine(
                new GridEventConsumeHandler((IgniteBiPredicate<UUID, Event>)locLsnr,
                    (IgnitePredicate<Event>)rmtFilter, types), bufSize, interval, autoUnsubscribe, prj.predicate()));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public void stopRemoteListen(UUID opId) {
        A.notNull(opId, "consumeId");

        guard();

        try {
            saveOrGet(ctx.continuous().stopRoutine(opId));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <T extends Event> T waitForLocal(@Nullable IgnitePredicate<T> filter,
        @Nullable int... types) {
        guard();

        try {
            return saveOrGet(ctx.event().waitForEvent(filter, types));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <T extends Event> Collection<T> localQuery(IgnitePredicate<T> p, @Nullable int... types) {
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
    @Override public void recordLocal(Event evt) {
        A.notNull(evt, "evt");

        if (evt.type() <= 1000)
            throw new IllegalArgumentException("All types in range from 1 to 1000 are reserved for " +
                "internal Ignite events [evtType=" + evt.type() + ", evt=" + evt + ']');

        guard();

        try {
            ctx.event().record(evt);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public void localListen(IgnitePredicate<? extends Event> lsnr, int[] types) {
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
    @Override public boolean stopLocalListen(IgnitePredicate<? extends Event> lsnr, @Nullable int... types) {
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
    private static <T extends Event> IgnitePredicate<T> compoundPredicate(final IgnitePredicate<T> p,
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
    @Override protected IgniteEvents createAsyncInstance() {
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