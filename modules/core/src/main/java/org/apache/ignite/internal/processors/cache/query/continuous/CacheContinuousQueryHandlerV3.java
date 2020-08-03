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

package org.apache.ignite.internal.processors.cache.query.continuous;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.query.ContinuousQueryWithTransformer;
import org.apache.ignite.cache.query.ContinuousQueryWithTransformer.EventListener;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteAsyncCallback;
import org.apache.ignite.lang.IgniteClosure;
import org.jetbrains.annotations.Nullable;

/**
 * Continuous query handler V3 version.
 * Contains {@link Factory} for remote transformer and {@link EventListener}.
 *
 * @see ContinuousQueryWithTransformer
 */
public class CacheContinuousQueryHandlerV3<K, V> extends CacheContinuousQueryHandlerV2<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Remote transformer. */
    private Factory<? extends IgniteClosure<CacheEntryEvent<? extends K, ? extends V>, ?>> rmtTransFactory;

    /** Deployable object for transformer. */
    private CacheContinuousQueryDeployableObject rmtTransFactoryDep;

    /** Remote transformer. */
    private transient IgniteClosure<CacheEntryEvent<? extends K, ? extends V>, ?> rmtTrans;

    /** Local listener for transformed events. */
    private transient EventListener<?> locTransLsnr;

    /**
     * Empty constructor.
     */
    public CacheContinuousQueryHandlerV3() {
        super();
    }

    /**
     * @param cacheName Cache name.
     * @param topic Topic.
     * @param locTransLsnr Local listener of transformed events
     * @param rmtFilterFactory Remote filter factory.
     * @param rmtTransFactory Remote transformer factory.
     * @param oldValRequired OldValRequired flag.
     * @param sync Sync flag.
     * @param ignoreExpired IgnoreExpired flag.
     * @param ignoreClsNotFound IgnoreClassNotFoundException flag.
     */
    public CacheContinuousQueryHandlerV3(
        String cacheName,
        Object topic,
        EventListener<?> locTransLsnr,
        @Nullable Factory<? extends CacheEntryEventFilter<K, V>> rmtFilterFactory,
        Factory<? extends IgniteClosure<CacheEntryEvent<? extends K, ? extends V>, ?>> rmtTransFactory,
        boolean oldValRequired,
        boolean sync,
        boolean ignoreExpired,
        boolean ignoreClsNotFound) {
        super(
            cacheName,
            topic,
            null,
            rmtFilterFactory,
            oldValRequired,
            sync,
            ignoreExpired,
            ignoreClsNotFound,
            null);

        assert rmtTransFactory != null;

        this.locTransLsnr = locTransLsnr;
        this.rmtTransFactory = rmtTransFactory;
    }

    /** {@inheritDoc} */
    @Override public IgniteClosure<CacheEntryEvent<? extends K, ? extends V>, ?> getTransformer() {
        if (rmtTrans == null && rmtTransFactory != null)
            rmtTrans = rmtTransFactory.create();

        return rmtTrans;
    }

    /** {@inheritDoc} */
    @Override public EventListener<?> localTransformedEventListener() {
        return locTransLsnr;
    }

    /** {@inheritDoc} */
    @Override protected CacheEntryEventFilter getEventFilter0() {
        if (rmtFilterFactory == null)
            return null;

        return super.getEventFilter0();
    }

    /** {@inheritDoc} */
    @Override public RegisterStatus register(UUID nodeId, UUID routineId,
        GridKernalContext ctx) throws IgniteCheckedException {
        final IgniteClosure trans = getTransformer();

        if (trans != null)
            ctx.resource().injectGeneric(trans);

        if (locTransLsnr != null) {
            ctx.resource().injectGeneric(locTransLsnr);

            asyncCb = U.hasAnnotation(locTransLsnr, IgniteAsyncCallback.class);
        }

        return super.register(nodeId, routineId, ctx);
    }

    /** {@inheritDoc} */
    @Override public void p2pMarshal(GridKernalContext ctx) throws IgniteCheckedException {
        super.p2pMarshal(ctx);

        if (rmtTransFactory != null && !U.isGrid(rmtTransFactory.getClass()))
            rmtTransFactoryDep = new CacheContinuousQueryDeployableObject(rmtTransFactory, ctx);
    }

    /** {@inheritDoc} */
    @Override public void p2pUnmarshal(UUID nodeId, GridKernalContext ctx) throws IgniteCheckedException {
        if (rmtTransFactoryDep != null)
            rmtTransFactory = p2pUnmarshal(rmtTransFactoryDep, nodeId, ctx);

        super.p2pUnmarshal(nodeId, ctx);
    }

    /** {@inheritDoc} */
    @Override public boolean isMarshalled() {
        return super.isMarshalled() &&
            (rmtTransFactory == null || U.isGrid(rmtTransFactory.getClass()) || rmtTransFactoryDep != null);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        boolean b = rmtTransFactoryDep != null;

        out.writeBoolean(b);

        if (b)
            out.writeObject(rmtTransFactoryDep);
        else
            out.writeObject(rmtTransFactory);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        boolean b = in.readBoolean();

        if (b) {
            rmtTransFactoryDep = (CacheContinuousQueryDeployableObject)in.readObject();

            if (p2pUnmarshalFut.isDone())
                p2pUnmarshalFut = new GridFutureAdapter<>();
        } else
            rmtTransFactory = (Factory<? extends IgniteClosure<CacheEntryEvent<? extends K, ? extends V>, ?>>)in.readObject();
    }
}
