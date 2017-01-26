/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite;

import java.util.Collection;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteAsyncSupport;
import org.apache.ignite.lang.IgniteAsyncSupported;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.services.ServiceDescriptor;
import org.jetbrains.annotations.Nullable;

/**
 * Defines functionality necessary to deploy distributed services on the grid.
 * <p>
 * Instance of {@code IgniteServices} which spans all cluster nodes can be obtained from Ignite as follows:
 * <pre class="brush:java">
 * Ignite ignite = Ignition.ignite();
 *
 * IgniteServices svcs = ignite.services();
 * </pre>
 * You can also obtain an instance of the services facade over a specific cluster group:
 * <pre class="brush:java">
 * // Cluster group over remote nodes (excluding the local node).
 * ClusterGroup remoteNodes = ignite.cluster().forRemotes();
 *
 * // Services instance spanning all remote cluster nodes.
 * IgniteServices svcs = ignite.services(remoteNodes);
 * </pre>
 * <p>
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
 * you can also automatically deploy services on startup by specifying them in {@link IgniteConfiguration}
 * like so:
 * <pre name="code" class="java">
 * IgniteConfiguration cfg = new IgniteConfiguration();
 *
 * ServiceConfiguration svcCfg1 = new ServiceConfiguration();
 *
 * // Cluster-wide singleton configuration.
 * svcCfg1.setName("myClusterSingletonService");
 * svcCfg1.setMaxPerNodeCount(1);
 * svcCfg1.setTotalCount(1);
 * svcCfg1.setService(new MyClusterSingletonService());
 *
 * ServiceConfiguration svcCfg2 = new ServiceConfiguration();
 *
 * // Per-node singleton configuration.
 * svcCfg2.setName("myNodeSingletonService");
 * svcCfg2.setMaxPerNodeCount(1);
 * svcCfg2.setService(new MyNodeSingletonService());
 *
 * cfg.setServiceConfiguration(svcCfg1, svcCfg2);
 * ...
 * Ignition.start(cfg);
 * </pre>
 * <h1 class="header">Load Balancing</h1>
 * In all cases, other than singleton service deployment, Ignite will automatically make sure that
 * an about equal number of services are deployed on each node within the grid. Whenever cluster topology
 * changes, Ignite will re-evaluate service deployments and may re-deploy an already deployed service
 * on another node for better load balancing.
 * <h1 class="header">Fault Tolerance</h1>
 * Ignite guarantees that services are deployed according to specified configuration regardless
 * of any topology changes, including node crashes.
 * <h1 class="header">Resource Injection</h1>
 * All distributed services can be injected with
 * ignite resources. Both, field and method based injections are supported. The following ignite
 * resources can be injected:
 * <ul>
 * <li>{@link IgniteInstanceResource}</li>
 * <li>{@link org.apache.ignite.resources.LoggerResource}</li>
 * <li>{@link org.apache.ignite.resources.SpringApplicationContextResource}</li>
 * <li>{@link org.apache.ignite.resources.SpringResource}</li>
 * </ul>
 * Refer to corresponding resource documentation for more information.
 * <h1 class="header">Service Example</h1>
 * Here is an example of how an distributed service may be implemented and deployed:
 * <pre name="code" class="java">
 * // Simple service implementation.
 * public class MyIgniteService implements Service {
 *      ...
 *      // Example of ignite resource injection. All resources are optional.
 *      // You should inject resources only as needed.
 *      &#64;IgniteInstanceResource
 *      private Ignite ignite;
 *      ...
 *      &#64;Override public void cancel(ServiceContext ctx) {
 *          // No-op.
 *      }
 *
 *      &#64;Override public void execute(ServiceContext ctx) {
 *          // Loop until service is cancelled.
 *          while (!ctx.isCancelled()) {
 *              // Do something.
 *              ...
 *          }
 *      }
 *  }
 * ...
 * IgniteServices svcs = ignite.services();
 *
 * svcs.deployClusterSingleton("mySingleton", new MyIgniteService());
 * </pre>
 */
public interface IgniteServices extends IgniteAsyncSupport {
    /**
     * Gets the cluster group to which this {@code IgniteServices} instance belongs.
     *
     * @return Cluster group to which this {@code IgniteServices} instance belongs.
     */
    public ClusterGroup clusterGroup();

    /**
     * Deploys a cluster-wide singleton service. Ignite will guarantee that there is always
     * one instance of the service in the cluster. In case if grid node on which the service
     * was deployed crashes or stops, Ignite will automatically redeploy it on another node.
     * However, if the node on which the service is deployed remains in topology, then the
     * service will always be deployed on that node only, regardless of topology changes.
     * <p>
     * Note that in case of topology changes, due to network delays, there may be a temporary situation
     * when a singleton service instance will be active on more than one node (e.g. crash detection delay).
     * <p>
     * This method is analogous to calling
     * {@link #deployMultiple(String, org.apache.ignite.services.Service, int, int) deployMultiple(name, svc, 1, 1)} method.
     *
     * @param name Service name.
     * @param svc Service instance.
     * @throws IgniteException If failed to deploy service.
     */
    @IgniteAsyncSupported
    public void deployClusterSingleton(String name, Service svc) throws IgniteException;

    /**
     * Deploys a per-node singleton service. Ignite will guarantee that there is always
     * one instance of the service running on each node. Whenever new nodes are started
     * within the underlying cluster group, Ignite will automatically deploy one instance of
     * the service on every new node.
     * <p>
     * This method is analogous to calling
     * {@link #deployMultiple(String, org.apache.ignite.services.Service, int, int) deployMultiple(name, svc, 0, 1)} method.
     *
     * @param name Service name.
     * @param svc Service instance.
     * @throws IgniteException If failed to deploy service.
     */
    @IgniteAsyncSupported
    public void deployNodeSingleton(String name, Service svc) throws IgniteException;

    /**
     * Deploys one instance of this service on the primary node for a given affinity key.
     * Whenever topology changes and primary node assignment changes, Ignite will always
     * make sure that the service is undeployed on the previous primary node and deployed
     * on the new primary node.
     * <p>
     * Note that in case of topology changes, due to network delays, there may be a temporary situation
     * when a service instance will be active on more than one node (e.g. crash detection delay).
     * <p>
     * This method is analogous to the invocation of {@link #deploy(org.apache.ignite.services.ServiceConfiguration)} method
     * as follows:
     * <pre name="code" class="java">
     *     ServiceConfiguration cfg = new ServiceConfiguration();
     *
     *     cfg.setName(name);
     *     cfg.setService(svc);
     *     cfg.setCacheName(cacheName);
     *     cfg.setAffinityKey(affKey);
     *     cfg.setTotalCount(1);
     *     cfg.setMaxPerNodeCount(1);
     *
     *     ignite.services().deploy(cfg);
     * </pre>
     *
     * @param name Service name.
     * @param svc Service instance.
     * @param cacheName Name of the cache on which affinity for key should be calculated, {@code null} for
     *      default cache.
     * @param affKey Affinity cache key.
     * @throws IgniteException If failed to deploy service.
     */
    @IgniteAsyncSupported
    public void deployKeyAffinitySingleton(String name, Service svc, @Nullable String cacheName, Object affKey)
        throws IgniteException;

    /**
     * Deploys multiple instances of the service on the grid. Ignite will deploy a
     * maximum amount of services equal to {@code 'totalCnt'} parameter making sure that
     * there are no more than {@code 'maxPerNodeCnt'} service instances running
     * on each node. Whenever topology changes, Ignite will automatically rebalance
     * the deployed services within cluster to make sure that each node will end up with
     * about equal number of deployed instances whenever possible.
     * <p>
     * Note that at least one of {@code 'totalCnt'} or {@code 'maxPerNodeCnt'} parameters must have
     * value greater than {@code 0}.
     * <p>
     * This method is analogous to the invocation of {@link #deploy(org.apache.ignite.services.ServiceConfiguration)} method
     * as follows:
     * <pre name="code" class="java">
     *     ServiceConfiguration cfg = new ServiceConfiguration();
     *
     *     cfg.setName(name);
     *     cfg.setService(svc);
     *     cfg.setTotalCount(totalCnt);
     *     cfg.setMaxPerNodeCount(maxPerNodeCnt);
     *
     *     ignite.services().deploy(cfg);
     * </pre>
     *
     * @param name Service name.
     * @param svc Service instance.
     * @param totalCnt Maximum number of deployed services in the grid, {@code 0} for unlimited.
     * @param maxPerNodeCnt Maximum number of deployed services on each node, {@code 0} for unlimited.
     * @throws IgniteException If failed to deploy service.
     */
    @IgniteAsyncSupported
    public void deployMultiple(String name, Service svc, int totalCnt, int maxPerNodeCnt) throws IgniteException;

    /**
     * Deploys multiple instances of the service on the grid according to provided
     * configuration. Ignite will deploy a maximum amount of services equal to
     * {@link org.apache.ignite.services.ServiceConfiguration#getTotalCount() cfg.getTotalCount()}  parameter
     * making sure that there are no more than {@link org.apache.ignite.services.ServiceConfiguration#getMaxPerNodeCount() cfg.getMaxPerNodeCount()}
     * service instances running on each node. Whenever topology changes, Ignite will automatically rebalance
     * the deployed services within cluster to make sure that each node will end up with
     * about equal number of deployed instances whenever possible.
     * <p>
     * If {@link org.apache.ignite.services.ServiceConfiguration#getAffinityKey() cfg.getAffinityKey()} is not {@code null}, then Ignite
     * will deploy the service on the primary node for given affinity key. The affinity will be calculated
     * on the cache with {@link org.apache.ignite.services.ServiceConfiguration#getCacheName() cfg.getCacheName()} name.
     * <p>
     * If {@link org.apache.ignite.services.ServiceConfiguration#getNodeFilter() cfg.getNodeFilter()} is not {@code null}, then
     * Ignite will deploy service on all grid nodes for which the provided filter evaluates to {@code true}.
     * The node filter will be checked in addition to the underlying cluster group filter, or the
     * whole grid, if the underlying cluster group includes all the cluster nodes.
     * <p>
     * Note that at least one of {@code 'totalCnt'} or {@code 'maxPerNodeCnt'} parameters must have
     * value greater than {@code 0}.
     * <p>
     * Here is an example of creating service deployment configuration:
     * <pre name="code" class="java">
     *     ServiceConfiguration cfg = new ServiceConfiguration();
     *
     *     cfg.setName(name);
     *     cfg.setService(svc);
     *     cfg.setTotalCount(0); // Unlimited.
     *     cfg.setMaxPerNodeCount(2); // Deploy 2 instances of service on each node.
     *
     *     ignite.services().deploy(cfg);
     * </pre>
     *
     * @param cfg Service configuration.
     * @throws IgniteException If failed to deploy service.
     */
    @IgniteAsyncSupported
    public void deploy(ServiceConfiguration cfg) throws IgniteException;

    /**
     * Cancels service deployment. If a service with specified name was deployed on the grid,
     * then {@link org.apache.ignite.services.Service#cancel(org.apache.ignite.services.ServiceContext)} method will be called on it.
     * <p>
     * Note that Ignite cannot guarantee that the service exits from {@link org.apache.ignite.services.Service#execute(org.apache.ignite.services.ServiceContext)}
     * method whenever {@link org.apache.ignite.services.Service#cancel(org.apache.ignite.services.ServiceContext)} is called. It is up to the user to
     * make sure that the service code properly reacts to cancellations.
     * <p>
     * Supports asynchronous execution (see {@link IgniteAsyncSupport}).
     *
     * @param name Name of service to cancel.
     * @throws IgniteException If failed to cancel service.
     */
    @IgniteAsyncSupported
    public void cancel(String name) throws IgniteException;

    /**
     * Cancels all deployed services.
     * <p>
     * Note that depending on user logic, it may still take extra time for a service to
     * finish execution, even after it was cancelled.
     * <p>
     * Supports asynchronous execution (see {@link IgniteAsyncSupport}).
     *
     * @throws IgniteException If failed to cancel services.
     */
    @IgniteAsyncSupported
    public void cancelAll() throws IgniteException;

    /**
     * Gets metadata about all deployed services in the grid.
     *
     * @return Metadata about all deployed services in the grid.
     */
    public Collection<ServiceDescriptor> serviceDescriptors();

    /**
     * Gets locally deployed service with specified name.
     *
     * @param name Service name.
     * @param <T> Service type
     * @return Deployed service with specified name.
     */
    public <T> T service(String name);

    /**
     * Gets all locally deployed services with specified name.
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
     * @param sticky Whether or not Ignite should always contact the same remote
     *      service or try to load-balance between services.
     * @return Either proxy over remote service or local service if it is deployed locally.
     * @throws IgniteException If failed to create service proxy.
     */
    public <T> T serviceProxy(String name, Class<? super T> svcItf, boolean sticky) throws IgniteException;

    /**
     * Gets a remote handle on the service with timeout. If service is available locally,
     * then local instance is returned and timeout ignored, otherwise, a remote proxy is dynamically
     * created and provided for the specified service.
     *
     * @param name Service name.
     * @param svcItf Interface for the service.
     * @param sticky Whether or not Ignite should always contact the same remote
     *      service or try to load-balance between services.
     * @param timeout If greater than 0 created proxy will wait for service availability only specified time,
     *  and will limit remote service invocation time.
     * @return Either proxy over remote service or local service if it is deployed locally.
     * @throws IgniteException If failed to create service proxy.
     */
    public <T> T serviceProxy(String name, Class<? super T> svcItf, boolean sticky, long timeout) throws IgniteException;

    /** {@inheritDoc} */
    @Override public IgniteServices withAsync();
}
