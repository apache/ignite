/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.resource;

import org.apache.ignite.*;
import org.apache.ignite.compute.*;
import org.gridgain.grid.kernal.managers.deployment.*;

/**
 * Simple injector which wraps GridComputeJobContext resource object.
 */
public class GridResourceJobContextInjector extends GridResourceBasicInjector<ComputeJobContext> {
    /**
     * Creates GridComputeJobContext injector.
     *
     * @param rsrc GridComputeJobContext resource to inject.
     */
    GridResourceJobContextInjector(ComputeJobContext rsrc) {
        super(rsrc);
    }

    /** {@inheritDoc} */
    @Override public void inject(GridResourceField field, Object target, Class<?> depCls, GridDeployment dep)
        throws IgniteCheckedException {
        assert target != null;

        if (!(target instanceof ComputeTask))
            super.inject(field, target, depCls, dep);
    }

    /** {@inheritDoc} */
    @Override public void inject(GridResourceMethod mtd, Object target, Class<?> depCls, GridDeployment dep)
        throws IgniteCheckedException {
        assert target != null;

        if (!(target instanceof ComputeTask))
            super.inject(mtd, target, depCls, dep);
    }
}
