/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.resource;

import org.apache.ignite.*;
import org.apache.ignite.managed.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.managers.deployment.*;

import java.util.*;

/**
 * Grid service injector.
 */
public class GridResourceServiceInjector extends GridResourceBasicInjector<Collection<ManagedService>> {
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
        throws GridException {
        IgniteServiceResource ann = (IgniteServiceResource)field.getAnnotation();

        Class svcItf = ann.proxyInterface();

        Object svc;

        if (svcItf == Void.class)
            svc = ignite.managed().service(ann.serviceName());
        else
            svc = ignite.managed().serviceProxy(ann.serviceName(), svcItf, ann.proxySticky());

        if (svc != null)
            GridResourceUtils.inject(field.getField(), target, svc);
    }

    /** {@inheritDoc} */
    @Override public void inject(GridResourceMethod mtd, Object target, Class<?> depCls, GridDeployment dep)
        throws GridException {
        IgniteServiceResource ann = (IgniteServiceResource)mtd.getAnnotation();

        Class svcItf = ann.proxyInterface();

        Object svc;

        if (svcItf == Void.class)
            svc = ignite.managed().service(ann.serviceName());
        else
            svc = ignite.managed().serviceProxy(ann.serviceName(), svcItf, ann.proxySticky());

        Class<?>[] types = mtd.getMethod().getParameterTypes();

        if (types.length != 1)
            throw new GridException("Setter does not have single parameter of required type [type=" +
                svc.getClass().getName() + ", setter=" + mtd + ']');

        if (svc != null)
            GridResourceUtils.inject(mtd.getMethod(), target, svc);
    }
}
