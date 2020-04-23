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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryUpdatedListener;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryManager.JCacheQueryRemoteFilter;
import org.apache.ignite.internal.processors.continuous.GridContinuousHandler;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Continuous query handler V2 version. Contains {@link Factory} for remote listener.
 */
public class CacheContinuousQueryHandlerV2<K, V> extends CacheContinuousQueryHandler<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Remote filter factory. */
    Factory<? extends CacheEntryEventFilter> rmtFilterFactory;

    /** Deployable object for filter factory. */
    private CacheContinuousQueryDeployableObject rmtFilterFactoryDep;

    /** Event types for JCache API. */
    private byte types;

    /** */
    protected transient CacheEntryEventFilter filter;

    /**
     * Required by {@link Externalizable}.
     */
    public CacheContinuousQueryHandlerV2() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param cacheName Cache name.
     * @param topic Topic for ordered messages.
     * @param locLsnr Local listener.
     * @param rmtFilterFactory Remote filter factory.
     * @param oldValRequired Old value required flag.
     * @param sync Synchronous flag.
     * @param ignoreExpired Ignore expired events flag.
     * @param types Event types.
     */
    public CacheContinuousQueryHandlerV2(
        String cacheName,
        Object topic,
        @Nullable CacheEntryUpdatedListener<K, V> locLsnr,
        @Nullable Factory<? extends CacheEntryEventFilter<K, V>> rmtFilterFactory,
        boolean oldValRequired,
        boolean sync,
        boolean ignoreExpired,
        boolean ignoreClsNotFound,
        @Nullable Byte types) {
        super(cacheName,
            topic,
            locLsnr,
            null,
            oldValRequired,
            sync,
            ignoreExpired,
            ignoreClsNotFound);
        this.rmtFilterFactory = rmtFilterFactory;

        if (types != null) {
            assert types != 0;

            this.types = types;
        }
    }

    /** {@inheritDoc} */
    @Override protected CacheEntryEventFilter getEventFilter0() {
        if (filter == null) {
            assert rmtFilterFactory != null;

            Factory<? extends CacheEntryEventFilter> factory = rmtFilterFactory;

            filter = factory.create();

            if (types != 0)
                filter = new JCacheQueryRemoteFilter(filter, types);
        }

        return filter;
    }

    /** {@inheritDoc} */
    @Override public void p2pMarshal(GridKernalContext ctx) throws IgniteCheckedException {
        super.p2pMarshal(ctx);

        if (rmtFilterFactory != null && !U.isGrid(rmtFilterFactory.getClass()))
            rmtFilterFactoryDep = new CacheContinuousQueryDeployableObject(rmtFilterFactory, ctx);
    }

    /** {@inheritDoc} */
    @Override public void p2pUnmarshal(UUID nodeId, GridKernalContext ctx) throws IgniteCheckedException {
        if (rmtFilterFactoryDep != null)
            rmtFilterFactory = p2pUnmarshal(rmtFilterFactoryDep, nodeId, ctx);

        super.p2pUnmarshal(nodeId, ctx);
    }

    /** {@inheritDoc} */
    @Override public boolean isMarshalled() {
        return super.isMarshalled() &&
            (rmtFilterFactory == null || U.isGrid(rmtFilterFactory.getClass()) || rmtFilterFactoryDep != null);
    }

    /** {@inheritDoc} */
    @Override public GridContinuousHandler clone() {
        return super.clone();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheContinuousQueryHandlerV2.class, this);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        boolean b = rmtFilterFactoryDep != null;

        out.writeBoolean(b);

        if (b)
            out.writeObject(rmtFilterFactoryDep);
        else
            out.writeObject(rmtFilterFactory);

        out.writeByte(types);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        boolean b = in.readBoolean();

        if (b) {
            rmtFilterFactoryDep = (CacheContinuousQueryDeployableObject)in.readObject();

            if (p2pUnmarshalFut.isDone())
                p2pUnmarshalFut = new GridFutureAdapter<>();
        } else
            rmtFilterFactory = (Factory)in.readObject();

        types = in.readByte();
    }
}
