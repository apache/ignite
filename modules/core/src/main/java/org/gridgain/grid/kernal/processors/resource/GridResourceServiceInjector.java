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

        GridResourceUtils.inject(field.getField(), target, srvcs);
    }

    /** {@inheritDoc} */
    @Override public void inject(GridResourceMethod mtd, Object target, Class<?> depCls, GridDeployment dep)
        throws GridException {
        GridServiceResource ann = (GridServiceResource)mtd.getAnnotation();

        Collection<GridService> srvcs = grid.services().services(ann.serviceName());

        GridResourceUtils.inject(mtd.getMethod(), target, srvcs);
    }
}
