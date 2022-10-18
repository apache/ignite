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

package org.apache.ignite.ioc.internal.processors.resource;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.resource.GridInjectResourceContext;
import org.apache.ignite.internal.processors.resource.GridResourceInjector;
import org.apache.ignite.ioc.Registry;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link GridInjectResourceContext}.
 */
public class GridInjectResourceContextImpl implements GridInjectResourceContext {
    /** Spring application context injector. */
    private GridResourceInjector beanInjector;
    private Registry registry;

    /**
     * @param registry Spring application context.
     */
    public GridInjectResourceContextImpl(@Nullable Registry registry) {
        beanInjector = new GridResourceIocBeanInjector(registry);
        this.registry = registry;
    }

    @Override
    public GridResourceInjector beanInjector() {
        return beanInjector;
    }

    /** {@inheritDoc} */
    @Override
    public Object unwrapTarget(Object target) throws IgniteCheckedException {
        return registry.unwrapTarget(target);
    }
}
