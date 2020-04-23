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
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteMessaging;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.cluster.ClusterGroupAdapter;
import org.apache.ignite.internal.processors.continuous.GridContinuousHandler;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteFuture;
import org.jetbrains.annotations.Nullable;

/**
 * {@link IgniteMessaging} implementation.
 */
public class IgniteMessagingImpl extends AsyncSupportAdapter<IgniteMessaging>
    implements IgniteMessaging, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private GridKernalContext ctx;

    /** */
    private ClusterGroupAdapter prj;

    /**
     * Required by {@link Externalizable}.
     */
    public IgniteMessagingImpl() {
        // No-op.
    }

    /**
     * @param ctx Kernal context.
     * @param prj Projection.
     * @param async Async support flag.
     */
    public IgniteMessagingImpl(GridKernalContext ctx, ClusterGroupAdapter prj, boolean async) {
        super(async);

        this.ctx = ctx;
        this.prj = prj;
    }

    /** {@inheritDoc} */
    @Override public ClusterGroup clusterGroup() {
        return prj;
    }

    /** {@inheritDoc} */
    @Override public void send(@Nullable Object topic, Object msg) {
       send0(topic, msg, isAsync());
    }

    /**
     * Implementation of send.
     * @param topic Topic.
     * @param msg Message.
     * @param async Async flag.
     * @throws IgniteException On error.
     */
    private void send0(@Nullable Object topic, Object msg, boolean async) throws IgniteException {
        A.notNull(msg, "msg");

        guard();

        try {
            Collection<ClusterNode> snapshot = prj.nodes();

            if (snapshot.isEmpty())
                throw U.emptyTopologyException();

            ctx.io().sendUserMessage(snapshot, msg, topic, false, 0, async);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public void send(@Nullable Object topic, Collection<?> msgs) {
        send0(topic, msgs, isAsync());
    }

    /**
     * Implementation of send.
     * @param topic Topic.
     * @param msgs Messages.
     * @param async Async flag.
     * @throws IgniteException On error.
     */
    private void send0(@Nullable Object topic, Collection<?> msgs, boolean async) throws IgniteException {
        A.ensure(!F.isEmpty(msgs), "msgs cannot be null or empty");

        guard();

        try {
            Collection<ClusterNode> snapshot = prj.nodes();

            if (snapshot.isEmpty())
                throw U.emptyTopologyException();

            for (Object msg : msgs) {
                A.notNull(msg, "msg");

                ctx.io().sendUserMessage(snapshot, msg, topic, false, 0, async);
            }
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public void sendOrdered(@Nullable Object topic, Object msg, long timeout) {
        A.notNull(msg, "msg");

        guard();

        try {
            Collection<ClusterNode> snapshot = prj.nodes();

            if (snapshot.isEmpty())
                throw U.emptyTopologyException();

            if (timeout == 0)
                timeout = ctx.config().getNetworkTimeout();

            ctx.io().sendUserMessage(snapshot, msg, topic, true, timeout, false);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public void localListen(@Nullable Object topic, IgniteBiPredicate<UUID, ?> p) {
        A.notNull(p, "p");

        guard();

        try {
            ctx.io().addUserMessageListener(topic, p);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public void stopLocalListen(@Nullable Object topic, IgniteBiPredicate<UUID, ?> p) {
        A.notNull(p, "p");

        guard();

        try {
            ctx.io().removeUserMessageListener(topic, p);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public UUID remoteListen(@Nullable Object topic, IgniteBiPredicate<UUID, ?> p) {
        A.notNull(p, "p");

        guard();

        try {
            GridContinuousHandler hnd = new GridMessageListenHandler(topic, securityAwareBiPredicate(p));

            return saveOrGet(ctx.continuous().startRoutine(hnd,
                false,
                1,
                0,
                false,
                prj.predicate()));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            unguard();
        }
    }

    /**
     * @param p Original IgniteBiPredicate.
     * @return Security aware IgniteBiPredicate.
     */
    private IgniteBiPredicate<UUID, Object> securityAwareBiPredicate(IgniteBiPredicate<UUID, ?> p) {
        IgniteBiPredicate<UUID, Object> res = (IgniteBiPredicate<UUID, Object>)p;

        if (ctx.security().enabled()) {
            final UUID subjectId = ctx.security().securityContext().subject().id();

            return new SecurityAwareBiPredicate<>(subjectId, res);
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<UUID> remoteListenAsync(@Nullable Object topic,
        IgniteBiPredicate<UUID, ?> p) throws IgniteException {
        A.notNull(p, "p");

        guard();

        try {
            GridContinuousHandler hnd = new GridMessageListenHandler(topic, securityAwareBiPredicate(p));

            return new IgniteFutureImpl<>(ctx.continuous().startRoutine(hnd,
                false,
                1,
                0,
                false,
                prj.predicate()));
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
        A.notNull(opId, "opId");

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
    @Override public IgniteFuture<Void> stopRemoteListenAsync(UUID opId) throws IgniteException {
        A.notNull(opId, "opId");

        guard();

        try {
            return (IgniteFuture<Void>)new IgniteFutureImpl<>(ctx.continuous().stopRoutine(opId));
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

    /** {@inheritDoc} */
    @Override protected IgniteMessaging createAsyncInstance() {
        return new IgniteMessagingImpl(ctx, prj, true);
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
        return prj.message();
    }
}
