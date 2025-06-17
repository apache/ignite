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
import org.apache.ignite.internal.managers.deployment.GridDeployment;
import org.apache.ignite.resources.SpringApplicationContextResource;

/**
 * {@link GridInjectResourceContext} that serves as a bridge from the deprecated {@link GridSpringResourceContext}.
 * GridInjectResourceContext doesn't know difference between spring application context and regular beans,
 * that's why this bridge checks field's and method's annotation whether it is {@link SpringApplicationContextResource}.
 */
public class GridSpringResourceContextBridge implements GridInjectResourceContext {
    /** Spring resource context. */
    private final GridSpringResourceContext rsrcCtx;

    /**
     * Constructor.
     *
     * @param ctx Spring resource context.
     */
    public GridSpringResourceContextBridge(GridSpringResourceContext ctx) {
        assert ctx != null;

        rsrcCtx = ctx;
    }

    /** {@inheritDoc} */
    @Override public GridResourceInjector beanInjector() {
        return new Injector();
    }

    /** {@inheritDoc} */
    @Override public Object unwrapTarget(Object target) throws IgniteCheckedException {
        return rsrcCtx.unwrapTarget(target);
    }

    /** Resource injector that handles spring-related annotations. */
    private class Injector implements GridResourceInjector {
        /** Bean injector. */
        private final GridResourceInjector beanInjector;

        /** Spring context injector. */
        private final GridResourceInjector contextInjector;

        /** Constructor. */
        public Injector() {
            beanInjector = rsrcCtx.springBeanInjector();
            contextInjector = rsrcCtx.springContextInjector();
        }

        /** {@inheritDoc} */
        @Override public void inject(GridResourceField field, Object target, Class<?> depCls,
            GridDeployment dep) throws IgniteCheckedException {
            if (field.getAnnotation() instanceof SpringApplicationContextResource)
                contextInjector.inject(field, target, depCls, dep);
            else
                beanInjector.inject(field, target, depCls, dep);
        }

        /** {@inheritDoc} */
        @Override public void inject(GridResourceMethod mtd, Object target, Class<?> depCls,
            GridDeployment dep) throws IgniteCheckedException {
            if (mtd.getAnnotation() instanceof SpringApplicationContextResource)
                contextInjector.inject(mtd, target, depCls, dep);
            else
                beanInjector.inject(mtd, target, depCls, dep);
        }

        /** {@inheritDoc} */
        @Override public void undeploy(GridDeployment dep) {
            // No-op as in the GridResourceSpringBeanInjector.
        }
    }
}
