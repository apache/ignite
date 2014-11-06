/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.service;

import java.io.*;

/**
 * An instance of grid-managed service. Grid-managed services may be deployed from
 * {@link GridServices} facade or directly from grid configuration at startup.
 * <h1 class="header">Deployment</h1>
 * Whenever service is deployed, GridGain will automatically calculate how many
 * instances of this service should be deployed on each node within the cluster.
 * Whenever service is deployed on a cluster node, GridGain will call
 * {@link #execute(GridServiceContext)} method on that service. It is up to the user
 * to control whenever the service should exit from the {@code execute} method.
 * For example, user may choose to implement service as follows:
 * <pre name="code" class="java">
 * public class MyGridService implements GridService {
 *      ...
 *      // Example of grid resource injection. All resources are optional.
 *      // You should inject resources only as needed.
 *      &#64;GridInstanceResource
 *      private Grid grid;
 *      ...
 *      &#64;Override public void cancel(GridServiceContext ctx) {
 *          // No-op.
 *      }
 *
 *      &#64;Override public void execute(GridServiceContext ctx) {
 *          // Loop until service is cancelled.
 *          while (!ctx.isCancelled()) {
 *              // Do something.
 *              ...
 *          }
 *      }
 *  }
 * </pre>
 * Consecutively, this service can be deployed as follows:
 * <pre name="code" class="java">
 * ...
 * GridServices svcs = grid.services();
 *
 * GridFuture&lt;?&gt; fut = svcs.deployClusterSingleton("mySingleton", new MyGridService());
 *
 * // Wait for deployment to complete.
 * fut.get();
 * </pre>
 * Or from grid configuration on startup:
 * <pre name="code" class="java">
 * GridConfiguration gridCfg = new GridConfiguration();
 *
 * GridServiceConfiguration svcCfg = new GridServiceConfiguration();
 *
 * // Configuration for cluster-singleton service.
 * svcCfg.setName("mySingleton");
 * svcCfg.setMaxPerNodeCount(1);
 * svcCfg.setTotalCount(1);
 * svcCfg.setService(new MyGridService());
 *
 * gridCfg.setServiceConfiguration(svcCfg);
 * ...
 * GridGain.start(gridCfg);
 * </pre>
 * <h1 class="header">Cancellation</h1>
 * Services can be cancelled by calling any of the {@code cancel} methods on {@link GridServices} API.
 * Whenever a deployed service is cancelled, GridGain will automatically call
 * {@link GridService#cancel(GridServiceContext)} method on that service.
 * <p>
 * Note that GridGain cannot guarantee that the service exits from {@link GridService#execute(GridServiceContext)}
 * method whenever {@link #cancel(GridServiceContext)} is called. It is up to the user to
 * make sure that the service code properly reacts to cancellations.
 */
public interface GridService extends Serializable {
    /**
     * Cancels this service. GridGain will automatically call this method whenever any of the
     * {@code cancel} methods on {@link GridServices} API are called.
     * <p>
     * Note that GridGain cannot guarantee that the service exits from {@link #execute(GridServiceContext)}
     * method whenever {@code cancel(GridServiceContext)} method is called. It is up to the user to
     * make sure that the service code properly reacts to cancellations.
     *
     * @param ctx Service execution context.
     */
    public void cancel(GridServiceContext ctx);

    /**
     * Starts execution of this service. This method is automatically invoked whenever an instance of the service
     * is deployed on a grid node. Note that service is considered deployed even after it exits the {@code execute}
     * method and can be cancelled (or undeployed) only by calling any of the {@code cancel} methods on
     * {@link GridServices} API. Also not that service is not required to exit from {@code execute} method until
     * {@link #cancel(GridServiceContext)} method was called.
     *
     * @param ctx Service execution context.
     * @throws Exception If service execution failed. Not that service will still remain deployed, until
     *      {@link GridServices#cancel(String)} method will be called.
     */
    public void execute(GridServiceContext ctx) throws Exception;
}
