/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.resource;

import org.apache.ignite.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.kernal.managers.deployment.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.springframework.context.*;

import java.io.*;
import java.lang.reflect.*;

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
        IgniteSpringResource ann = (IgniteSpringResource)field.getAnnotation();

        assert ann != null;

        // Note: injected non-serializable user resources should not mark
        // injected spring beans with transient modifier.

        // Check for 'transient' modifier only in serializable classes.
        if (!Modifier.isTransient(field.getField().getModifiers()) &&
            Serializable.class.isAssignableFrom(field.getField().getDeclaringClass())) {
            throw new IgniteCheckedException("@GridSpringResource must only be used with 'transient' fields: " +
                field.getField());
        }

        String name = ann.resourceName();

        if (springCtx != null) {
            Object bean = springCtx.getBean(name);

            GridResourceUtils.inject(field.getField(), target, bean);
        }
    }

    /** {@inheritDoc} */
    @Override public void inject(GridResourceMethod mtd, Object target, Class<?> cls,
        GridDeployment depCls) throws IgniteCheckedException {
        IgniteSpringResource ann = (IgniteSpringResource)mtd.getAnnotation();

        assert ann != null;

        if (mtd.getMethod().getParameterTypes().length != 1)
            throw new IgniteCheckedException("Method injection setter must have only one parameter: " + mtd.getMethod());

        String name = ann.resourceName();

        if (springCtx != null) {
            Object bean = springCtx.getBean(name);

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
}
