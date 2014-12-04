/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.loadtests.dsi;

import org.apache.ignite.Ignite;
import org.apache.ignite.lifecycle.*;
import org.gridgain.grid.*;
import org.apache.ignite.resources.GridInstanceResource;
import org.apache.ignite.resources.GridSpringApplicationContextResource;
import org.springframework.context.ApplicationContext;

/**
 *
 */
public class GridDsiLifecycleBean implements LifecycleBean {
    /**
     * Grid instance will be automatically injected. For additional resources
     * that can be injected into lifecycle beans see
     * {@link org.apache.ignite.lifecycle.LifecycleBean} documentation.
     */
    @GridInstanceResource
    private Ignite ignite;

    /** */
    @SuppressWarnings("UnusedDeclaration")
    @GridSpringApplicationContextResource
    private ApplicationContext springCtx;

    /** {@inheritDoc} */
    @Override public void onLifecycleEvent(LifecycleEventType evt) throws GridException {
        switch (evt) {
            case BEFORE_GRID_START:
                break;

            case AFTER_GRID_START:
                ignite.cache("PARTITIONED_CACHE").dataStructures().atomicSequence("ID", 0, true);
                break;

            case BEFORE_GRID_STOP:
                break;

            case AFTER_GRID_STOP:
                break;
        }
    }
}
