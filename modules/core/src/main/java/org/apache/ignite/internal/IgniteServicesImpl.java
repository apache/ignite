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
import java.util.Collections;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteServices;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.internal.cluster.ClusterGroupAdapter;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.services.ServiceDescriptor;
import org.jetbrains.annotations.Nullable;

/**
 * {@link org.apache.ignite.IgniteServices} implementation.
 */
@SuppressWarnings("unchecked")
public class IgniteServicesImpl extends AsyncSupportAdapter implements IgniteServices, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private GridKernalContext ctx;

    /** */
    private ClusterGroupAdapter prj;

    /**
     * Required by {@link Externalizable}.
     */
    public IgniteServicesImpl() {
        // No-op.
    }

    /**
     * @param ctx Kernal context.
     * @param prj Projection.
     * @param async Async support flag.
     */
    public IgniteServicesImpl(GridKernalContext ctx, ClusterGroupAdapter prj, boolean async) {
        super(async);

        this.ctx = ctx;
        this.prj = prj;
    }

    /** {@inheritDoc} */
    @Override public ClusterGroup clusterGroup() {
        return prj;
    }

    /** {@inheritDoc} */
    @Override public void deployNodeSingleton(String name, Service svc) {
        A.notNull(name, "name");
        A.notNull(svc, "svc");

        guard();

        try {
            saveOrGet(ctx.service().deployNodeSingleton(prj, name, svc));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> deployNodeSingletonAsync(String name, Service svc) {
        A.notNull(name, "name");
        A.notNull(svc, "svc");

        guard();

        try {
            return (IgniteFuture<Void>)new IgniteFutureImpl<>(ctx.service().deployNodeSingleton(prj, name, svc));
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public void deployClusterSingleton(String name, Service svc) {
        A.notNull(name, "name");
        A.notNull(svc, "svc");

        guard();

        try {
            saveOrGet(ctx.service().deployClusterSingleton(prj, name, svc));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> deployClusterSingletonAsync(String name, Service svc) {
        A.notNull(name, "name");
        A.notNull(svc, "svc");

        guard();

        try {
            return (IgniteFuture<Void>)new IgniteFutureImpl<>(ctx.service().deployClusterSingleton(prj, name, svc));
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public void deployMultiple(String name, Service svc, int totalCnt, int maxPerNodeCnt) {
        A.notNull(name, "name");
        A.notNull(svc, "svc");

        guard();

        try {
            saveOrGet(ctx.service().deployMultiple(prj, name, svc, totalCnt, maxPerNodeCnt));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> deployMultipleAsync(String name, Service svc, int totalCnt, int maxPerNodeCnt) {
        A.notNull(name, "name");
        A.notNull(svc, "svc");

        guard();

        try {
            return (IgniteFuture<Void>)new IgniteFutureImpl<>(ctx.service().deployMultiple(prj, name, svc,
                totalCnt, maxPerNodeCnt));
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public void deployKeyAffinitySingleton(String name, Service svc, @Nullable String cacheName,
        Object affKey) {
        A.notNull(name, "name");
        A.notNull(svc, "svc");
        A.notNull(affKey, "affKey");

        guard();

        try {
            saveOrGet(ctx.service().deployKeyAffinitySingleton(name, svc, cacheName, affKey));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> deployKeyAffinitySingletonAsync(String name, Service svc,
        @Nullable String cacheName, Object affKey) {
        A.notNull(name, "name");
        A.notNull(svc, "svc");
        A.notNull(affKey, "affKey");

        guard();

        try {
            return (IgniteFuture<Void>)new IgniteFutureImpl<>(ctx.service().deployKeyAffinitySingleton(name, svc,
                cacheName, affKey));
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public void deploy(ServiceConfiguration cfg) {
        A.notNull(cfg, "cfg");

        deployAll(Collections.singleton(cfg), false);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> deployAsync(ServiceConfiguration cfg) {
        A.notNull(cfg, "cfg");

        return deployAllAsync(Collections.singleton(cfg), false);
    }

    /** {@inheritDoc} */
    @Override public void deployAll(Collection<ServiceConfiguration> cfgs, boolean allOrNone) {
        A.notNull(cfgs, "cfgs");

        guard();

        try {
            saveOrGet(ctx.service().deployAll(cfgs, allOrNone));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> deployAllAsync(Collection<ServiceConfiguration> cfgs,
        boolean allOrNone) {
        A.notNull(cfgs, "cfgs");

        guard();

        try {
            return (IgniteFuture<Void>)new IgniteFutureImpl<>(ctx.service().deployAll(cfgs, allOrNone));
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public void cancel(String name) {
        A.notNull(name, "name");

        guard();

        try {
            saveOrGet(ctx.service().cancel(name));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> cancelAsync(String name) {
        A.notNull(name, "name");

        guard();

        try {
            return (IgniteFuture<Void>)new IgniteFutureImpl<>(ctx.service().cancel(name));
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public void cancelAll(Collection<String> names) {
        guard();

        try {
            saveOrGet(ctx.service().cancelAll(names));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> cancelAllAsync(Collection<String> names) {
        guard();

        try {
            return (IgniteFuture<Void>)new IgniteFutureImpl<>(ctx.service().cancelAll(names));
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public void cancelAll() {
        guard();

        try {
            saveOrGet(ctx.service().cancelAll());
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> cancelAllAsync() {
        guard();

        try {
            return (IgniteFuture<Void>)new IgniteFutureImpl<>(ctx.service().cancelAll());
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<ServiceDescriptor> serviceDescriptors() {
        guard();

        try {
            return ctx.service().serviceDescriptors();
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <T> T service(String name) {
        guard();

        try {
            return ctx.service().service(name);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <T> T serviceProxy(String name, Class<? super T> svcItf, boolean sticky)
        throws IgniteException {
        return (T) serviceProxy(name, svcItf, sticky, 0);
    }

    /** {@inheritDoc} */
    @Override public <T> T serviceProxy(final String name, final Class<? super T> svcItf, final boolean sticky,
        final long timeout) throws IgniteException {
        A.notNull(name, "name");
        A.notNull(svcItf, "svcItf");
        A.ensure(svcItf.isInterface(), "Service class must be an interface: " + svcItf);
        A.ensure(timeout >= 0, "Timeout cannot be negative: " + timeout);

        guard();

        try {
            return (T)ctx.service().serviceProxy(prj, name, svcItf, sticky, timeout);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <T> Collection<T> services(String name) {
        guard();

        try {
            return ctx.service().services(name);
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
    @Override public IgniteServices withAsync() {
        if (isAsync())
            return this;

        return new IgniteServicesImpl(ctx, prj, true);
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
        return prj.services();
    }
}
