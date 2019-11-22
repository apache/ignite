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

import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.internal.managers.deployment.GridDeployment;
import org.apache.ignite.lang.IgniteBiClosure;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteRunnable;

import static org.apache.ignite.internal.processors.security.sandbox.SandboxIgniteComponentProxy.proxy;

/** Ignite instance injector. */
public class GridResourceProxiedIgniteInjector extends GridResourceBasicInjector<Ignite> {
    /** Array of classes that should get a proxied instance of Ignite. */
    private static final Class[] PROXIED_CLASSES = new Class[]{
        Runnable.class,
        IgniteRunnable.class,
        Callable.class,
        IgniteCallable.class,
        ComputeTask.class,
        ComputeJob.class,
        IgniteClosure.class,
        IgniteBiClosure.class,
        IgniteDataStreamer.class,
        IgnitePredicate.class,
        IgniteBiPredicate.class,
    };

    /**
     * @param rsrc Resource.
     */
    public GridResourceProxiedIgniteInjector(Ignite rsrc) {
        super(rsrc);
    }

    /** */
    private Ignite ignite(Object target) {
        return shouldUseProxy(target) ? proxy(Ignite.class, getResource()) : getResource();
    }

    /**
     * @return True if {@code target} should get a proxy instance of Ignite.
     */
    private boolean shouldUseProxy(Object target){
        for(Class cls : PROXIED_CLASSES){
            if (cls.isInstance(target))
                return true;
        }

        return false;
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
