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

/**
 * Resource injector implementations contain logic and resources that
 * should be injected for selected target objects.
 *
 * @author @java.author
 * @version @java.version
 */
interface GridResourceInjector {
    /**
     * Injects resource into field. Caches injected resource with the given key if needed.
     *
     * @param field Field to inject.
     * @param target Target object the field belongs to.
     * @param depCls Deployed class.
     * @param dep Deployment.
     * @throws GridException If injection failed.
     */
    public void inject(GridResourceField field, Object target, Class<?> depCls, GridDeployment dep) throws GridException;

    /**
     * Injects resource with a setter method. Caches injected resource with the given key if needed.
     *
     * @param mtd Setter method.
     * @param target Target object the field belongs to.
     * @param depCls Deployed class.
     * @param dep Deployment.
     * @throws GridException If injection failed.
     */
    public void inject(GridResourceMethod mtd, Object target, Class<?> depCls, GridDeployment dep) throws GridException;

    /**
     * Gracefully cleans all resources associated with deployment.
     *
     * @param dep Deployment to undeploy.
     */
    public void undeploy(GridDeployment dep);
}
