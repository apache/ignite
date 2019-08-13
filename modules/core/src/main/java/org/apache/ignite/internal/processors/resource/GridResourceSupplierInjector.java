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

import java.util.function.Supplier;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.managers.deployment.GridDeployment;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Simple injector which wraps resource object supplier.
 *
 * @param <T> Type of injected resource.
 */
class GridResourceSupplierInjector<T> implements GridResourceInjector {
    /** Resource to inject. */
    private final Supplier<T> supplier;

    /**
     * Creates injector.
     *
     * @param supplier Resource supplier.
     */
    GridResourceSupplierInjector(Supplier<T> supplier) {
        this.supplier = supplier;
    }

    /** {@inheritDoc} */
    @Override public void inject(GridResourceField field, Object target, Class<?> depCls, GridDeployment dep)
        throws IgniteCheckedException {
        GridResourceUtils.inject(field.getField(), target, supplier.get());
    }

    /** {@inheritDoc} */
    @Override public void inject(GridResourceMethod mtd, Object target, Class<?> depCls, GridDeployment dep)
        throws IgniteCheckedException {
        GridResourceUtils.inject(mtd.getMethod(), target, supplier.get());
    }

    /** {@inheritDoc} */
    @Override public void undeploy(GridDeployment dep) {
        /* No-op. There is no cache. */
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridResourceSupplierInjector.class, this);
    }
}