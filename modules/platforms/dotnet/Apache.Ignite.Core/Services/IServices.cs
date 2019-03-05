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

namespace Apache.Ignite.Core.Services
{
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Cluster;

    /// <summary>
    /// Defines functionality to deploy distributed services in the Ignite.
    /// </summary>
    public interface IServices
    {
        /// <summary>
        /// Gets the cluster group to which this instance belongs.
        /// </summary>
        /// <value>
        /// The cluster group to which this instance belongs.
        /// </value>
        IClusterGroup ClusterGroup { get; }

        /// <summary>
        /// Deploys a cluster-wide singleton service. Ignite guarantees that there is always
        /// one instance of the service in the cluster. In case if Ignite node on which the service
        /// was deployed crashes or stops, Ignite will automatically redeploy it on another node.
        /// However, if the node on which the service is deployed remains in topology, then the
        /// service will always be deployed on that node only, regardless of topology changes.
        /// <para />
        /// Note that in case of topology changes, due to network delays, there may be a temporary situation
        /// when a singleton service instance will be active on more than one node (e.g. crash detection delay).
        /// </summary>
        /// <param name="name">Service name.</param>
        /// <param name="service">Service instance.</param>
        void DeployClusterSingleton(string name, IService service);

        /// <summary>
        /// Deploys a cluster-wide singleton service. Ignite guarantees that there is always
        /// one instance of the service in the cluster. In case if Ignite node on which the service
        /// was deployed crashes or stops, Ignite will automatically redeploy it on another node.
        /// However, if the node on which the service is deployed remains in topology, then the
        /// service will always be deployed on that node only, regardless of topology changes.
        /// <para />
        /// Note that in case of topology changes, due to network delays, there may be a temporary situation
        /// when a singleton service instance will be active on more than one node (e.g. crash detection delay).
        /// </summary>
        /// <param name="name">Service name.</param>
        /// <param name="service">Service instance.</param>
        Task DeployClusterSingletonAsync(string name, IService service);

        /// <summary>
        /// Deploys a per-node singleton service. Ignite guarantees that there is always
        /// one instance of the service running on each node. Whenever new nodes are started
        /// within the underlying cluster group, Ignite will automatically deploy one instance of
        /// the service on every new node.
        /// </summary>
        /// <param name="name">Service name.</param>
        /// <param name="service">Service instance.</param>
        void DeployNodeSingleton(string name, IService service);

        /// <summary>
        /// Deploys a per-node singleton service. Ignite guarantees that there is always
        /// one instance of the service running on each node. Whenever new nodes are started
        /// within the underlying cluster group, Ignite will automatically deploy one instance of
        /// the service on every new node.
        /// </summary>
        /// <param name="name">Service name.</param>
        /// <param name="service">Service instance.</param>
        Task DeployNodeSingletonAsync(string name, IService service);

        /// <summary>
        /// Deploys one instance of this service on the primary node for a given affinity key.
        /// Whenever topology changes and primary node assignment changes, Ignite will always
        /// make sure that the service is undeployed on the previous primary node and deployed
        /// on the new primary node.
        /// <para />
        /// Note that in case of topology changes, due to network delays, there may be a temporary situation
        /// when a service instance will be active on more than one node (e.g. crash detection delay).
        /// </summary>
        /// <param name="name">Service name.</param>
        /// <param name="service">Service instance.</param>
        /// <param name="cacheName">Name of the cache on which affinity for key should be calculated, null for
        /// default cache.</param>
        /// <param name="affinityKey">Affinity cache key.</param>
        void DeployKeyAffinitySingleton<TK>(string name, IService service, string cacheName, TK affinityKey);

        /// <summary>
        /// Deploys one instance of this service on the primary node for a given affinity key.
        /// Whenever topology changes and primary node assignment changes, Ignite will always
        /// make sure that the service is undeployed on the previous primary node and deployed
        /// on the new primary node.
        /// <para />
        /// Note that in case of topology changes, due to network delays, there may be a temporary situation
        /// when a service instance will be active on more than one node (e.g. crash detection delay).
        /// </summary>
        /// <param name="name">Service name.</param>
        /// <param name="service">Service instance.</param>
        /// <param name="cacheName">Name of the cache on which affinity for key should be calculated, null for
        /// default cache.</param>
        /// <param name="affinityKey">Affinity cache key.</param>
        Task DeployKeyAffinitySingletonAsync<TK>(string name, IService service, string cacheName, TK affinityKey);

        /// <summary>
        /// Deploys multiple instances of the service on the grid. Ignite will deploy a
        /// maximum amount of services equal to <paramref name="totalCount" /> parameter making sure that
        /// there are no more than <paramref name="maxPerNodeCount" /> service instances running
        /// on each node. Whenever topology changes, Ignite will automatically rebalance
        /// the deployed services within cluster to make sure that each node will end up with
        /// about equal number of deployed instances whenever possible.
        /// </summary>
        /// <param name="name">Service name.</param>
        /// <param name="service">Service instance.</param>
        /// <param name="totalCount">Maximum number of deployed services in the grid, 0 for unlimited.</param>
        /// <param name="maxPerNodeCount">Maximum number of deployed services on each node, 0 for unlimited.</param>
        void DeployMultiple(string name, IService service, int totalCount, int maxPerNodeCount);

        /// <summary>
        /// Deploys multiple instances of the service on the grid. Ignite will deploy a
        /// maximum amount of services equal to <paramref name="totalCount" /> parameter making sure that
        /// there are no more than <paramref name="maxPerNodeCount" /> service instances running
        /// on each node. Whenever topology changes, Ignite will automatically rebalance
        /// the deployed services within cluster to make sure that each node will end up with
        /// about equal number of deployed instances whenever possible.
        /// </summary>
        /// <param name="name">Service name.</param>
        /// <param name="service">Service instance.</param>
        /// <param name="totalCount">Maximum number of deployed services in the grid, 0 for unlimited.</param>
        /// <param name="maxPerNodeCount">Maximum number of deployed services on each node, 0 for unlimited.</param>
        Task DeployMultipleAsync(string name, IService service, int totalCount, int maxPerNodeCount);

        /// <summary>
        /// Deploys instances of the service in the Ignite according to provided configuration.
        /// </summary>
        /// <param name="configuration">Service configuration.</param>
        void Deploy(ServiceConfiguration configuration);

        /// <summary>
        /// Deploys instances of the service in the Ignite according to provided configuration.
        /// </summary>
        /// <param name="configuration">Service configuration.</param>
        Task DeployAsync(ServiceConfiguration configuration);

        /// <summary>
        /// Deploys multiple services described by provided configurations. Depending on specified parameters, 
        /// multiple instances of the same service may be deployed. Whenever topology changes,
        /// Ignite will automatically rebalance the deployed services within cluster to make sure that each node
        /// will end up with about equal number of deployed instances whenever possible.
        /// <para/>
        /// If deployment of some of the provided services fails, then <see cref="ServiceDeploymentException"/> 
        /// containing a list of failed service configurations 
        /// (<see cref="ServiceDeploymentException.FailedConfigurations"/>) will be thrown. It is guaranteed that all 
        /// services  that were provided to this method and are not present in the list of failed services are 
        /// successfully deployed by the moment of the exception being thrown.
        /// Note that if exception is thrown, then partial deployment may have occurred.
        /// </summary>
        /// <param name="configurations">Collection of service configurations to be deployed.</param>
        void DeployAll(IEnumerable<ServiceConfiguration> configurations);

        /// <summary>
        /// Asynchronously deploys multiple services described by provided configurations. Depending on specified 
        /// parameters, multiple instances of the same service may be deployed (<see cref="ServiceConfiguration"/>).
        /// Whenever topology changes, Ignite will automatically rebalance the deployed services within cluster to make
        /// sure that each node  will end up with about equal number of deployed instances whenever possible.
        /// <para/>
        /// If deployment of some of the provided services fails, then <see cref="ServiceDeploymentException"/> 
        /// containing a list of failed service configurations 
        /// (<see cref="ServiceDeploymentException.FailedConfigurations"/>) will be thrown. It is guaranteed that all 
        /// services, that were provided to this method and are not present in the list of failed services, are 
        /// successfully deployed by the moment of the exception being thrown.
        /// Note that if exception is thrown, then partial deployment may have occurred.
        /// </summary>
        /// <param name="configurations">Collection of service configurations to be deployed.</param>
        Task DeployAllAsync(IEnumerable<ServiceConfiguration> configurations);

        /// <summary>
        /// Cancels service deployment. If a service with specified name was deployed on the grid,
        /// then <see cref="IService.Cancel"/> method will be called on it.
        /// <para/>
        /// Note that Ignite cannot guarantee that the service exits from <see cref="IService.Execute"/>
        /// method whenever <see cref="IService.Cancel"/> is called. It is up to the user to
        /// make sure that the service code properly reacts to cancellations.
        /// </summary>
        /// <param name="name">Name of the service to cancel.</param>
        void Cancel(string name);

        /// <summary>
        /// Cancels service deployment. If a service with specified name was deployed on the grid,
        /// then <see cref="IService.Cancel"/> method will be called on it.
        /// <para/>
        /// Note that Ignite cannot guarantee that the service exits from <see cref="IService.Execute"/>
        /// method whenever <see cref="IService.Cancel"/> is called. It is up to the user to
        /// make sure that the service code properly reacts to cancellations.
        /// </summary>
        /// <param name="name">Name of the service to cancel.</param>
        Task CancelAsync(string name);

        /// <summary>
        /// Cancels all deployed services.
        /// <para/>
        /// Note that depending on user logic, it may still take extra time for a service to
        /// finish execution, even after it was cancelled.
        /// </summary>
        void CancelAll();

        /// <summary>
        /// Cancels all deployed services.
        /// <para/>
        /// Note that depending on user logic, it may still take extra time for a service to
        /// finish execution, even after it was cancelled.
        /// </summary>
        Task CancelAllAsync();

        /// <summary>
        /// Gets metadata about all deployed services.
        /// </summary>
        /// <returns>Metadata about all deployed services.</returns>
        [SuppressMessage("Microsoft.Design", "CA1024:UsePropertiesWhereAppropriate",
            Justification = "Expensive operation.")]
        ICollection<IServiceDescriptor> GetServiceDescriptors();

        /// <summary>
        /// Gets deployed service with specified name.
        /// </summary>
        /// <typeparam name="T">Service type.</typeparam>
        /// <param name="name">Service name.</param>
        /// <returns>Deployed service with specified name.</returns>
        T GetService<T>(string name);

        /// <summary>
        /// Gets all deployed services with specified name.
        /// </summary>
        /// <typeparam name="T">Service type.</typeparam>
        /// <param name="name">Service name.</param>
        /// <returns>All deployed services with specified name.</returns>
        ICollection<T> GetServices<T>(string name);

        /// <summary>
        /// Gets a remote handle on the service. If service is available locally,
        /// then local instance is returned, otherwise, a remote proxy is dynamically
        /// created and provided for the specified service.
        /// </summary>
        /// <typeparam name="T">Service type.</typeparam>
        /// <param name="name">Service name.</param>
        /// <returns>Either proxy over remote service or local service if it is deployed locally.</returns>
        T GetServiceProxy<T>(string name) where T : class;

        /// <summary>
        /// Gets a remote handle on the service. If service is available locally,
        /// then local instance is returned, otherwise, a remote proxy is dynamically
        /// created and provided for the specified service.
        /// </summary>
        /// <typeparam name="T">Service type.</typeparam>
        /// <param name="name">Service name.</param>
        /// <param name="sticky">Whether or not Ignite should always contact the same remote
        /// service or try to load-balance between services.</param>
        /// <returns>Either proxy over remote service or local service if it is deployed locally.</returns>
        T GetServiceProxy<T>(string name, bool sticky) where T : class;

        /// <summary>
        /// Gets a remote handle on the service as a dynamic object. If service is available locally,
        /// then local instance is returned, otherwise, a remote proxy is dynamically
        /// created and provided for the specified service.
        /// <para />
        /// This method utilizes <c>dynamic</c> feature of the language and does not require any
        /// service interfaces or classes. Java services can be accessed as well as .NET services.
        /// </summary>
        /// <param name="name">Service name.</param>
        /// <returns>Either proxy over remote service or local service if it is deployed locally.</returns>
        dynamic GetDynamicServiceProxy(string name);

        /// <summary>
        /// Gets a remote handle on the service as a dynamic object. If service is available locally,
        /// then local instance is returned, otherwise, a remote proxy is dynamically
        /// created and provided for the specified service.
        /// <para />
        /// This method utilizes <c>dynamic</c> feature of the language and does not require any
        /// service interfaces or classes. Java services can be accessed as well as .NET services.
        /// </summary>
        /// <param name="name">Service name.</param>
        /// <param name="sticky">Whether or not Ignite should always contact the same remote
        /// service or try to load-balance between services.</param>
        /// <returns>Either proxy over remote service or local service if it is deployed locally.</returns>
        dynamic GetDynamicServiceProxy(string name, bool sticky);

        /// <summary>
        /// Returns an instance with binary mode enabled.
        /// Service method results will be kept in binary form.
        /// </summary>
        /// <returns>Instance with binary mode enabled.</returns>
        IServices WithKeepBinary();

        /// <summary>
        /// Returns an instance with server-side binary mode enabled.
        /// Service method arguments will be kept in binary form.
        /// </summary>
        /// <returns>Instance with server-side binary mode enabled.</returns>
        IServices WithServerKeepBinary();
    }
}