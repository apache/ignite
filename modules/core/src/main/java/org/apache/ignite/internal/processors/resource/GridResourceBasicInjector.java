/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.resource;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.managers.deployment.GridDeployment;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Simple injector which wraps only one resource object.
 * @param <T> Type of injected resource.
 */
class GridResourceBasicInjector<T> implements GridResourceInjector {
    /** Resource to inject. */
    private final T rsrc;

    /**
     * Creates injector.
     *
     * @param rsrc Resource to inject.
     */
    GridResourceBasicInjector(T rsrc) {
        this.rsrc = rsrc;
    }

    /**
     * Gets resource.
     *
     * @return Resource
     */
    public T getResource() {
        return rsrc;
    }

    /** {@inheritDoc} */
    @Override public void inject(GridResourceField field, Object target, Class<?> depCls, GridDeployment dep)
        throws IgniteCheckedException {
        GridResourceUtils.inject(field.getField(), target, rsrc);
    }

    /** {@inheritDoc} */
    @Override public void inject(GridResourceMethod mtd, Object target, Class<?> depCls, GridDeployment dep)
        throws IgniteCheckedException {
        GridResourceUtils.inject(mtd.getMethod(), target, rsrc);
    }

    /** {@inheritDoc} */
    @Override public void undeploy(GridDeployment dep) {
        /* No-op. There is no cache. */
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridResourceBasicInjector.class, this);
    }
}