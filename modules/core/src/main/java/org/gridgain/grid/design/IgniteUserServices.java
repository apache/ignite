/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.design;

import org.gridgain.grid.*;
import org.gridgain.grid.design.userservices.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.service.*;

import java.util.*;

/**
 * Defines functionality necessary to deploy distributed services on the grid. Instance of
 * {@code GridServices} is obtained from grid projection as follows:
 * <pre name="code" class="java">
 * GridServices svcs = GridGain.grid().services();
 * </pre>
 * With distributed services you can do the following:
 * <ul>
 * <li>Automatically deploy any number of service instances on the grid.</li>
 * <li>
 *     Automatically deploy singletons, including <b>cluster-singleton</b>,
 *     <b>node-singleton</b>, or <b>key-affinity-singleton</b>.
 * </li>
 * <li>Automatically deploy services on node start-up by specifying them in grid configuration.</li>
 * <li>Undeploy any of the deployed services.</li>
 * <li>Get information about service deployment topology within the grid.</li>
 * </ul>
 * <h1 class="header">Deployment From Configuration</h1>
 * In addition to deploying managed services by calling any of the provided {@code deploy(...)} methods,
 * you can also automatically deploy services on startup by specifying them in {@link GridConfiguration}
 * like so:
 * <pre name="code" class="java">
 * GridConfiguration gridCfg = new GridConfiguration();
 *
 * GridServiceConfiguration svcCfg1 = new GridServiceConfiguration();
 *
 * // Cluster-wide singleton configuration.
 * svcCfg1.setName("myClusterSingletonService");
 * svcCfg1.setMaxPerNodeCount(1);
 * svcCfg1.setTotalCount(1);
 * svcCfg1.setService(new MyClusterSingletonService());
 *
 * GridServiceConfiguration svcCfg2 = new GridServiceConfiguration();
 *
 * // Per-node singleton configuration.
 * svcCfg2.setName("myNodeSingletonService");
 * svcCfg2.setMaxPerNodeCount(1);
 * svcCfg2.setService(new MyNodeSingletonService());
 *
 * gridCfg.setServiceConfiguration(svcCfg1, svcCfg2);
 * ...
 * GridGain.start(gridCfg);
 * </pre>
 * <h1 class="header">Load Balancing</h1>
 * In all cases, other than singleton service deployment, GridGain will automatically make sure that
 * an about equal number of services are deployed on each node within the grid. Whenever cluster topology
 * changes, GridGain will re-evaluate service deployments and may re-deploy an already deployed service
 * on another node for better load balancing.
 * <h1 class="header">Fault Tolerance</h1>
 * GridGain guarantees that services are deployed according to specified configuration regardless
 * of any topology changes, including node crashes.
 * <h1 class="header">Resource Injection</h1>
 * All distributed services can be injected with
 * grid resources. Both, field and method based injections are supported. The following grid
 * resources can be injected:
 * <ul>
 * <li>{@link GridInstanceResource}</li>
 * <li>{@link GridLoggerResource}</li>
 * <li>{@link GridHomeResource}</li>
 * <li>{@link GridExecutorServiceResource}</li>
 * <li>{@link GridLocalNodeIdResource}</li>
 * <li>{@link GridMBeanServerResource}</li>
 * <li>{@link GridMarshallerResource}</li>
 * <li>{@link GridSpringApplicationContextResource}</li>
 * <li>{@link GridSpringResource}</li>
 * </ul>
 * Refer to corresponding resource documentation for more information.
 * <h1 class="header">Service Example</h1>
 * Here is an example of how an distributed service may be implemented and deployed:
 * <pre name="code" class="java">
 * // Simple service implementation.
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
 * ...
 * GridServices svcs = grid.services();
 *
 * GridFuture&lt;?&gt; fut = svcs.deployClusterSingleton("mySingleton", new MyGridService());
 *
 * // Wait for deployment to complete.
 * fut.get();
 * </pre>
 */
public interface IgniteUserServices extends UserServicesTopology {
    /** {@inheritDoc} */
    @Override IgniteUserServices enableAsync();

    /**
     * Cancels service deployment. If a service with specified name was deployed on the grid,
     * then {@link GridService#cancel(GridServiceContext)} method will be called on it.
     * <p>
     * Note that GridGain cannot guarantee that the service exits from {@link GridService#execute(GridServiceContext)}
     * method whenever {@link GridService#cancel(GridServiceContext)} is called. It is up to the user to
     * make sure that the service code properly reacts to cancellations.
     *
     * @param name Name of service to cancel.
     */
    public void cancel(String name);

    /**
     * Cancels all deployed services.
     * <p>
     * Note that GridGain cannot guarantee that the service exits from {@link GridService#execute(GridServiceContext)}
     * method whenever {@link GridService#cancel(GridServiceContext)} is called. It is up to the user to
     * make sure that the service code properly reacts to cancellations.
     */
    public void cancelAll();

    /**
     * Gets metadata about all deployed services.
     *
     * @return Metadata about all deployed services.
     */
    public Collection<GridServiceDescriptor> serviceDescriptors();

    /**
     * Gets deployed service with specified name.
     *
     * @param name Service name.
     * @param <T> Service type
     * @return Deployed service with specified name.
     */
    public <T> T localService(String name);

    /**
     * Gets all deployed services with specified name.
     *
     * @param name Service name.
     * @param <T> Service type.
     * @return all deployed services with specified name.
     */
    public <T> Collection<T> localServices(String name);
}
