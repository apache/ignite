/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite;

import org.apache.ignite.cluster.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.managed.*;
import org.gridgain.grid.*;
import org.jetbrains.annotations.*;

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
 * you can also automatically deploy services on startup by specifying them in {@link org.apache.ignite.configuration.IgniteConfiguration}
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
 * <li>{@link org.apache.ignite.resources.IgniteInstanceResource}</li>
 * <li>{@link org.apache.ignite.resources.IgniteLoggerResource}</li>
 * <li>{@link org.apache.ignite.resources.IgniteHomeResource}</li>
 * <li>{@link org.apache.ignite.resources.IgniteExecutorServiceResource}</li>
 * <li>{@link org.apache.ignite.resources.IgniteLocalNodeIdResource}</li>
 * <li>{@link org.apache.ignite.resources.IgniteMBeanServerResource}</li>
 * <li>{@link org.apache.ignite.resources.IgniteMarshallerResource}</li>
 * <li>{@link org.apache.ignite.resources.IgniteSpringApplicationContextResource}</li>
 * <li>{@link org.apache.ignite.resources.IgniteSpringResource}</li>
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
public interface IgniteManaged extends IgniteAsyncSupport {
    /**
     * Gets grid projection to which this {@code GridServices} instance belongs.
     *
     * @return Grid projection to which this {@code GridServices} instance belongs.
     */
    public ClusterGroup clusterGroup();

    /**
     * Deploys a cluster-wide singleton service. GridGain will guarantee that there is always
     * one instance of the service in the cluster. In case if grid node on which the service
     * was deployed crashes or stops, GridGain will automatically redeploy it on another node.
     * However, if the node on which the service is deployed remains in topology, then the
     * service will always be deployed on that node only, regardless of topology changes.
     * <p>
     * Note that in case of topology changes, due to network delays, there may be a temporary situation
     * when a singleton service instance will be active on more than one node (e.g. crash detection delay).
     * <p>
     * This method is analogous to calling
     * {@link #deployMultiple(String, org.apache.ignite.managed.ManagedService, int, int) deployMultiple(name, svc, 1, 1)} method.
     * <p>
     * Supports asynchronous execution (see {@link IgniteAsyncSupport}).
     *
     * @param name Service name.
     * @param svc Service instance.
     * @throws GridException If failed to deploy service.
     */
    public void deployClusterSingleton(String name, ManagedService svc) throws GridException;

    /**
     * Deploys a per-node singleton service. GridGain will guarantee that there is always
     * one instance of the service running on each node. Whenever new nodes are started
     * within this grid projection, GridGain will automatically deploy one instance of
     * the service on every new node.
     * <p>
     * This method is analogous to calling
     * {@link #deployMultiple(String, org.apache.ignite.managed.ManagedService, int, int) deployMultiple(name, svc, 0, 1)} method.
     * <p>
     * Supports asynchronous execution (see {@link IgniteAsyncSupport}).
     *
     * @param name Service name.
     * @param svc Service instance.
     * @throws GridException If failed to deploy service.
     */
    public void deployNodeSingleton(String name, ManagedService svc) throws GridException;

    /**
     * Deploys one instance of this service on the primary node for a given affinity key.
     * Whenever topology changes and primary node assignment changes, GridGain will always
     * make sure that the service is undeployed on the previous primary node and deployed
     * on the new primary node.
     * <p>
     * Note that in case of topology changes, due to network delays, there may be a temporary situation
     * when a service instance will be active on more than one node (e.g. crash detection delay).
     * <p>
     * This method is analogous to the invocation of {@link #deploy(org.apache.ignite.managed.ManagedServiceConfiguration)} method
     * as follows:
     * <pre name="code" class="java">
     *     GridServiceConfiguration cfg = new GridServiceConfiguration();
     *
     *     cfg.setName(name);
     *     cfg.setService(svc);
     *     cfg.setCacheName(cacheName);
     *     cfg.setAffinityKey(affKey);
     *     cfg.setTotalCount(1);
     *     cfg.setMaxPerNodeCount(1);
     *
     *     grid.services().deploy(cfg);
     * </pre>
     * <p>
     * Supports asynchronous execution (see {@link IgniteAsyncSupport}).
     *
     * @param name Service name.
     * @param svc Service instance.
     * @param cacheName Name of the cache on which affinity for key should be calculated, {@code null} for
     *      default cache.
     * @param affKey Affinity cache key.
     * @throws GridException If failed to deploy service.
     */
    public void deployKeyAffinitySingleton(String name, ManagedService svc, @Nullable String cacheName, Object affKey)
        throws GridException;

    /**
     * Deploys multiple instances of the service on the grid. GridGain will deploy a
     * maximum amount of services equal to {@code 'totalCnt'} parameter making sure that
     * there are no more than {@code 'maxPerNodeCnt'} service instances running
     * on each node. Whenever topology changes, GridGain will automatically rebalance
     * the deployed services within cluster to make sure that each node will end up with
     * about equal number of deployed instances whenever possible.
     * <p>
     * Note that at least one of {@code 'totalCnt'} or {@code 'maxPerNodeCnt'} parameters must have
     * value greater than {@code 0}.
     * <p>
     * This method is analogous to the invocation of {@link #deploy(org.apache.ignite.managed.ManagedServiceConfiguration)} method
     * as follows:
     * <pre name="code" class="java">
     *     GridServiceConfiguration cfg = new GridServiceConfiguration();
     *
     *     cfg.setName(name);
     *     cfg.setService(svc);
     *     cfg.setTotalCount(totalCnt);
     *     cfg.setMaxPerNodeCount(maxPerNodeCnt);
     *
     *     grid.services().deploy(cfg);
     * </pre>
     * <p>
     * Supports asynchronous execution (see {@link IgniteAsyncSupport}).
     *
     * @param name Service name.
     * @param svc Service instance.
     * @param totalCnt Maximum number of deployed services in the grid, {@code 0} for unlimited.
     * @param maxPerNodeCnt Maximum number of deployed services on each node, {@code 0} for unlimited.
     * @throws GridException If failed to deploy service.
     */
    public void deployMultiple(String name, ManagedService svc, int totalCnt, int maxPerNodeCnt) throws GridException;

    /**
     * Deploys multiple instances of the service on the grid according to provided
     * configuration. GridGain will deploy a maximum amount of services equal to
     * {@link org.apache.ignite.managed.ManagedServiceConfiguration#getTotalCount() cfg.getTotalCount()}  parameter
     * making sure that there are no more than {@link org.apache.ignite.managed.ManagedServiceConfiguration#getMaxPerNodeCount() cfg.getMaxPerNodeCount()}
     * service instances running on each node. Whenever topology changes, GridGain will automatically rebalance
     * the deployed services within cluster to make sure that each node will end up with
     * about equal number of deployed instances whenever possible.
     * <p>
     * If {@link org.apache.ignite.managed.ManagedServiceConfiguration#getAffinityKey() cfg.getAffinityKey()} is not {@code null}, then GridGain
     * will deploy the service on the primary node for given affinity key. The affinity will be calculated
     * on the cache with {@link org.apache.ignite.managed.ManagedServiceConfiguration#getCacheName() cfg.getCacheName()} name.
     * <p>
     * If {@link org.apache.ignite.managed.ManagedServiceConfiguration#getNodeFilter() cfg.getNodeFilter()} is not {@code null}, then
     * GridGain will deploy service on all grid nodes for which the provided filter evaluates to {@code true}.
     * The node filter will be checked in addition to the underlying grid projection filter, or the
     * whole grid, if the underlying grid projection includes all grid nodes.
     * <p>
     * Note that at least one of {@code 'totalCnt'} or {@code 'maxPerNodeCnt'} parameters must have
     * value greater than {@code 0}.
     * <p>
     * Supports asynchronous execution (see {@link IgniteAsyncSupport}).
     * <p>
     * Here is an example of creating service deployment configuration:
     * <pre name="code" class="java">
     *     GridServiceConfiguration cfg = new GridServiceConfiguration();
     *
     *     cfg.setName(name);
     *     cfg.setService(svc);
     *     cfg.setTotalCount(0); // Unlimited.
     *     cfg.setMaxPerNodeCount(2); // Deploy 2 instances of service on each node.
     *
     *     grid.services().deploy(cfg);
     * </pre>
     *
     * @param cfg Service configuration.
     * @throws GridException If failed to deploy service.
     */
    public void deploy(ManagedServiceConfiguration cfg) throws GridException;

    /**
     * Cancels service deployment. If a service with specified name was deployed on the grid,
     * then {@link org.apache.ignite.managed.ManagedService#cancel(org.apache.ignite.managed.ManagedServiceContext)} method will be called on it.
     * <p>
     * Note that GridGain cannot guarantee that the service exits from {@link org.apache.ignite.managed.ManagedService#execute(org.apache.ignite.managed.ManagedServiceContext)}
     * method whenever {@link org.apache.ignite.managed.ManagedService#cancel(org.apache.ignite.managed.ManagedServiceContext)} is called. It is up to the user to
     * make sure that the service code properly reacts to cancellations.
     * <p>
     * Supports asynchronous execution (see {@link IgniteAsyncSupport}).
     *
     * @param name Name of service to cancel.
     * @throws GridException If failed to cancel service.
     */
    public void cancel(String name) throws GridException;

    /**
     * Cancels all deployed services.
     * <p>
     * Note that depending on user logic, it may still take extra time for a service to
     * finish execution, even after it was cancelled.
     * <p>
     * Supports asynchronous execution (see {@link IgniteAsyncSupport}).
     *
     * @throws GridException If failed to cancel services.
     */
    public void cancelAll() throws GridException;

    /**
     * Gets metadata about all deployed services.
     *
     * @return Metadata about all deployed services.
     */
    public Collection<ManagedServiceDescriptor> deployedServices();

    /**
     * Gets deployed service with specified name.
     *
     * @param name Service name.
     * @param <T> Service type
     * @return Deployed service with specified name.
     */
    public <T> T service(String name);

    /**
     * Gets all deployed services with specified name.
     *
     * @param name Service name.
     * @param <T> Service type.
     * @return all deployed services with specified name.
     */
    public <T> Collection<T> services(String name);

    /**
     * Gets a remote handle on the service. If service is available locally,
     * then local instance is returned, otherwise, a remote proxy is dynamically
     * created and provided for the specified service.
     *
     * @param name Service name.
     * @param svcItf Interface for the service.
     * @param sticky Whether or not GridGain should always contact the same remote
     *      service or try to load-balance between services.
     * @return Either proxy over remote service or local service if it is deployed locally.
     */
    public <T> T serviceProxy(String name, Class<? super T> svcItf, boolean sticky) throws GridRuntimeException;

    /** {@inheritDoc} */
    @Override public IgniteManaged enableAsync();
}
