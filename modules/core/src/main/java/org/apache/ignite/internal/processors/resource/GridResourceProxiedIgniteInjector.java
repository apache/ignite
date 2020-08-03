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

package org.apache.ignite.internal.processors.resource;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.deployment.GridDeployment;

import static org.apache.ignite.internal.processors.security.SecurityUtils.isSystemType;
import static org.apache.ignite.internal.processors.security.sandbox.SandboxIgniteComponentProxy.igniteProxy;

/** Ignite instance injector. */
public class GridResourceProxiedIgniteInjector extends GridResourceBasicInjector<Ignite> {
    /**
     * @param rsrc Resource.
     */
    public GridResourceProxiedIgniteInjector(Ignite rsrc) {
        super(rsrc);
    }

    /** */
    private Ignite ignite(Object target) {
        return isSystemType(((IgniteEx)getResource()).context(), target, false)
            ? getResource() : igniteProxy(getResource());
    }

    /** {@inheritDoc} */
    @Override public void inject(GridResourceField field, Object target, Class<?> depCls, GridDeployment dep)
        throws IgniteCheckedException {
        GridResourceUtils.inject(field.getField(), target, ignite(target));
    }

    /** {@inheritDoc} */
    @Override public void inject(GridResourceMethod mtd, Object target, Class<?> depCls, GridDeployment dep)
        throws IgniteCheckedException {
        GridResourceUtils.inject(mtd.getMethod(), target, ignite(target));
    }
}
