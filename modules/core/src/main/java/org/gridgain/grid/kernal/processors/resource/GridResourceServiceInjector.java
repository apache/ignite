/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.resource;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.managers.deployment.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.service.*;
import org.gridgain.grid.util.typedef.*;

import java.lang.reflect.*;
import java.util.*;

/**
 * Grid service injector.
 */
public class GridResourceServiceInjector extends GridResourceBasicInjector<Collection<GridService>> {
    /** */
    private Grid grid;

    public GridResourceServiceInjector(Grid grid) {
        super(null);

        this.grid = grid;
    }

    /** {@inheritDoc} */
    @Override public void inject(GridResourceField field, Object target, Class<?> depCls, GridDeployment dep)
        throws GridException {
        GridServiceResource ann = (GridServiceResource)field.getAnnotation();

        Collection<GridService> srvcs = grid.services().services(ann.serviceName());

        Field f = field.getField();

        if (Collection.class.equals(f.getType()))
            GridResourceUtils.inject(f, target, srvcs);
        else {
            if (Collection.class.isAssignableFrom(f.getType()))
                throw new GridException("Failed to inject resource because target field should have " +
                    "'Collection' type (no descendant classes are allowed).");

            GridResourceUtils.inject(f, target, F.isEmpty(srvcs) ? null : srvcs.iterator().next());
        }
    }

    /** {@inheritDoc} */
    @Override public void inject(GridResourceMethod mtd, Object target, Class<?> depCls, GridDeployment dep)
        throws GridException {
        GridServiceResource ann = (GridServiceResource)mtd.getAnnotation();

        Collection<GridService> srvcs = grid.services().services(ann.serviceName());

        Class<?>[] types = mtd.getMethod().getParameterTypes();

        if (types.length != 1)
            throw new GridException("Setter does not have single parameter of required type [type=" +
                srvcs.getClass().getName() + ", setter=" + mtd + ']');

        if (Collection.class.equals(types[0]))
            GridResourceUtils.inject(mtd.getMethod(), target, srvcs);
        else {
            if (Collection.class.isAssignableFrom(types[0]))
                throw new GridException("Failed to inject resource because target parameter should have " +
                    "'Collection' type (no descendant classes are allowed).");

            GridResourceUtils.inject(mtd.getMethod(), target, F.isEmpty(srvcs) ? null : srvcs.iterator().next());
        }
    }
}
