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

import java.io.Serializable;
import java.lang.reflect.Modifier;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.managers.deployment.GridDeployment;
import org.apache.ignite.internal.processors.resource.GridResourceField;
import org.apache.ignite.internal.processors.resource.GridResourceInjector;
import org.apache.ignite.internal.processors.resource.GridResourceMethod;
import org.apache.ignite.internal.processors.resource.GridResourceUtils;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.ioc.Registry;
import org.apache.ignite.resources.InjectResource;

/**
 * Grid resource bean injector implementation.
 *
 * This injector works with supplied {@link Registry} instance which can be selected at runtime be
 * developer or application itself.
 */
public class GridResourceIocBeanInjector implements GridResourceInjector {
    /** */
    private Registry registry;

    /**
     * Creates injector object.
     *
     * @param registry Bean registry.
     */
    public GridResourceIocBeanInjector(Registry registry) {
        this.registry = registry;
    }

    /** {@inheritDoc} */
    @Override public void inject(GridResourceField field, Object target, Class<?> cls,
        GridDeployment depCls) throws IgniteCheckedException {
        InjectResource ann = (InjectResource)field.getAnnotation();

        assert ann != null;

        // Note: injected non-serializable user resources should not mark
        // injected spring beans with transient modifier.

        // Check for 'transient' modifier only in serializable classes.
        if (!Modifier.isTransient(field.getField().getModifiers()) &&
            Serializable.class.isAssignableFrom(field.getField().getDeclaringClass())) {
            throw new IgniteCheckedException("@InjectResource must only be used with 'transient' fields: " +
                field.getField());
        }

        if (registry != null) {
            Object bean = getBeanByResourceAnnotation(ann);

            if (bean != null) {
                GridResourceUtils.inject(field.getField(), target, bean);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void inject(GridResourceMethod mtd, Object target, Class<?> cls,
        GridDeployment depCls) throws IgniteCheckedException {
        InjectResource ann = (InjectResource)mtd.getAnnotation();

        assert ann != null;

        if (mtd.getMethod().getParameterTypes().length != 1)
            throw new IgniteCheckedException("Method injection setter must have only one parameter: " + mtd.getMethod());

        if (registry != null) {
            Object bean = getBeanByResourceAnnotation(ann);

            if (bean != null) {
                GridResourceUtils.inject(mtd.getMethod(), target, bean);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void undeploy(GridDeployment dep) {
        /* No-op. There is no cache. */
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridResourceIocBeanInjector.class, this);
    }

    /**
     * Retrieves the bean specified by {@link InjectResource} annotation from {@link #registry}.
     *
     * @param annotation {@link InjectResource} annotation instance from field or method.
     * @return Bean object retrieved from registry context.
     * @throws IgniteCheckedException If failed.
     */
    private Object getBeanByResourceAnnotation(InjectResource annotation) throws IgniteCheckedException {
        assert registry != null;

        String beanName = annotation.resourceName();
        Class<?> beanCls = annotation.resourceClass();

        boolean oneParamSet = !beanName.trim().isEmpty() ^ beanCls != InjectResource.DEFAULT.class;

        if (!oneParamSet) {
            throw new IgniteCheckedException("Either bean name or its class must be specified in @InjectResource, " +
                "but not both");
        }

        try {
            return beanName.trim().isEmpty() ? registry.lookup(beanName) : registry.lookup(beanCls);
        } catch (Exception e) {
            if (annotation.required()) {
                throw new IgniteCheckedException(e);
            }
            return null;
        }
    }
}
