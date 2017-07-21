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

import org.apache.ignite.IgniteCheckedException;
import org.jetbrains.annotations.Nullable;
import org.springframework.aop.framework.Advised;
import org.springframework.context.ApplicationContext;

/**
 * Implementation of {@link GridSpringResourceContext}.
 */
public class GridSpringResourceContextImpl implements GridSpringResourceContext {
    /** Spring application context injector. */
    private GridResourceInjector springCtxInjector;

    /** Spring bean resources injector. */
    private GridResourceInjector springBeanInjector;

    /**
     * @param springCtx Spring application context.
     */
    public GridSpringResourceContextImpl(@Nullable ApplicationContext springCtx) {
        springCtxInjector = new GridResourceBasicInjector<>(springCtx);
        springBeanInjector = new GridResourceSpringBeanInjector(springCtx);
    }

    /** {@inheritDoc} */
    @Override public GridResourceInjector springBeanInjector() {
        return springBeanInjector;
    }

    /** {@inheritDoc} */
    @Override public GridResourceInjector springContextInjector() {
        return springCtxInjector;
    }

    /** {@inheritDoc} */
    @Override public Object unwrapTarget(Object target) throws IgniteCheckedException {
        if (target instanceof Advised) {
            try {
                return ((Advised)target).getTargetSource().getTarget();
            }
            catch (Exception e) {
                throw new IgniteCheckedException("Failed to unwrap Spring proxy target [cls=" + target.getClass().getName() +
                    ", target=" + target + ']', e);
            }
        }

        return target;
    }
}