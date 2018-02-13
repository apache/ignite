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

import java.io.Serializable;
import java.lang.reflect.Modifier;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.managers.deployment.GridDeployment;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.resources.SpringResource;
import org.springframework.context.ApplicationContext;
import org.springframework.util.StringUtils;

/**
 * Spring bean injector implementation works with resources provided
 * by Spring {@code ApplicationContext}.
 */
public class GridResourceSpringBeanInjector implements GridResourceInjector {
    /** */
    private ApplicationContext springCtx;

    /**
     * Creates injector object.
     *
     * @param springCtx Spring context.
     */
    public GridResourceSpringBeanInjector(ApplicationContext springCtx) {
        this.springCtx = springCtx;
    }

    /** {@inheritDoc} */
    @Override public void inject(GridResourceField field, Object target, Class<?> cls,
        GridDeployment depCls) throws IgniteCheckedException {
        SpringResource ann = (SpringResource)field.getAnnotation();

        assert ann != null;

        // Note: injected non-serializable user resources should not mark
        // injected spring beans with transient modifier.

        // Check for 'transient' modifier only in serializable classes.
        if (!Modifier.isTransient(field.getField().getModifiers()) &&
            Serializable.class.isAssignableFrom(field.getField().getDeclaringClass())) {
            throw new IgniteCheckedException("@SpringResource must only be used with 'transient' fields: " +
                field.getField());
        }

        if (springCtx != null) {
            Object bean = getBeanByResourceAnnotation(ann);

            GridResourceUtils.inject(field.getField(), target, bean);
        }
    }

    /** {@inheritDoc} */
    @Override public void inject(GridResourceMethod mtd, Object target, Class<?> cls,
        GridDeployment depCls) throws IgniteCheckedException {
        SpringResource ann = (SpringResource)mtd.getAnnotation();

        assert ann != null;

        if (mtd.getMethod().getParameterTypes().length != 1)
            throw new IgniteCheckedException("Method injection setter must have only one parameter: " + mtd.getMethod());

        if (springCtx != null) {
            Object bean = getBeanByResourceAnnotation(ann);

            GridResourceUtils.inject(mtd.getMethod(), target, bean);
        }
    }

    /** {@inheritDoc} */
    @Override public void undeploy(GridDeployment dep) {
        /* No-op. There is no cache. */
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridResourceSpringBeanInjector.class, this);
    }

    /**
     * Retrieves from {@link #springCtx} the bean specified by {@link SpringResource} annotation.
     *
     * @param annotation {@link SpringResource} annotation instance from field or method.
     * @return Bean object retrieved from spring context.
     * @throws IgniteCheckedException If failed.
     */
    private Object getBeanByResourceAnnotation(SpringResource annotation) throws IgniteCheckedException {
        assert springCtx != null;

        String beanName = annotation.resourceName();
        Class<?> beanCls = annotation.resourceClass();

        boolean oneParamSet = !StringUtils.isEmpty(beanName) ^ beanCls != SpringResource.DEFAULT.class;

        if (!oneParamSet) {
            throw new IgniteCheckedException("Either bean name or its class must be specified in @SpringResource, " +
                "but not both");
        }

        Object bean;

        if (!StringUtils.isEmpty(beanName))
            bean = springCtx.getBean(beanName);
        else
            bean = springCtx.getBean(beanCls);

        return bean;
    }
}