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

import java.util.Collection;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.managers.deployment.GridDeployment;
import org.apache.ignite.resources.ServiceResource;
import org.apache.ignite.services.Service;

/**
 * Grid service injector.
 */
public class GridResourceServiceInjector extends GridResourceBasicInjector<Collection<Service>> {
    /** */
    private Ignite ignite;

    /**
     * @param ignite Grid.
     */
    public GridResourceServiceInjector(Ignite ignite) {
        super(null);

        this.ignite = ignite;
    }

    /** {@inheritDoc} */
    @Override public void inject(GridResourceField field, Object target, Class<?> depCls, GridDeployment dep)
        throws IgniteCheckedException {
        ServiceResource ann = (ServiceResource)field.getAnnotation();

        Class svcItf = ann.proxyInterface();

        Object svc;

        if (svcItf == Void.class)
            svc = ignite.services().service(ann.serviceName());
        else
            svc = ignite.services().serviceProxy(ann.serviceName(), svcItf, ann.proxySticky());

        if (svc != null)
            GridResourceUtils.inject(field.getField(), target, svc);
    }

    /** {@inheritDoc} */
    @Override public void inject(GridResourceMethod mtd, Object target, Class<?> depCls, GridDeployment dep)
        throws IgniteCheckedException {
        ServiceResource ann = (ServiceResource)mtd.getAnnotation();

        Class svcItf = ann.proxyInterface();

        Object svc;

        if (svcItf == Void.class)
            svc = ignite.services().service(ann.serviceName());
        else
            svc = ignite.services().serviceProxy(ann.serviceName(), svcItf, ann.proxySticky());

        Class<?>[] types = mtd.getMethod().getParameterTypes();

        if (types.length != 1)
            throw new IgniteCheckedException("Setter does not have single parameter of required type [type=" +
                svc.getClass().getName() + ", setter=" + mtd + ']');

        if (svc != null)
            GridResourceUtils.inject(mtd.getMethod(), target, svc);
    }
}