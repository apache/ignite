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

namespace Apache.Ignite.Core.Tests.Services
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Services;

    /// <summary>
    /// Services async wrapper to simplify testing.
    /// </summary>
    public class ServicesAsyncWrapper : IServices
    {
        /** Wrapped async services. */
        private readonly IServices _services;

        /// <summary>
        /// Initializes a new instance of the <see cref="ServicesAsyncWrapper"/> class.
        /// </summary>
        /// <param name="services">Services to wrap.</param>
        public ServicesAsyncWrapper(IServices services)
        {
            _services = services;
        }

        /** <inheritDoc /> */
        public IClusterGroup ClusterGroup
        {
            get { return _services.ClusterGroup; }
        }

        /** <inheritDoc /> */
        public void DeployClusterSingleton(string name, IService service)
        {
            _services.DeployClusterSingletonAsync(name, service).Wait();
        }

        /** <inheritDoc /> */
        public Task DeployClusterSingletonAsync(string name, IService service)
        {
            return _services.DeployClusterSingletonAsync(name, service);
        }

        /** <inheritDoc /> */
        public void DeployNodeSingleton(string name, IService service)
        {
            _services.DeployNodeSingletonAsync(name, service).Wait();
        }

        /** <inheritDoc /> */
        public Task DeployNodeSingletonAsync(string name, IService service)
        {
            return _services.DeployNodeSingletonAsync(name, service);
        }

        /** <inheritDoc /> */
        public void DeployKeyAffinitySingleton<TK>(string name, IService service, string cacheName, TK affinityKey)
        {
            _services.DeployKeyAffinitySingletonAsync(name, service, cacheName, affinityKey).Wait();
        }

        /** <inheritDoc /> */
        public Task DeployKeyAffinitySingletonAsync<TK>(string name, IService service, string cacheName, TK affinityKey)
        {
            return _services.DeployKeyAffinitySingletonAsync(name, service, cacheName, affinityKey);
        }

        /** <inheritDoc /> */
        public void DeployMultiple(string name, IService service, int totalCount, int maxPerNodeCount)
        {
            try
            {
                _services.DeployMultipleAsync(name, service, totalCount, maxPerNodeCount).Wait();
            }
            catch (AggregateException ex)
            {
                throw ex.InnerException ?? ex;
            }
        }

        /** <inheritDoc /> */
        public Task DeployMultipleAsync(string name, IService service, int totalCount, int maxPerNodeCount)
        {
            return _services.DeployMultipleAsync(name, service, totalCount, maxPerNodeCount);
        }

        /** <inheritDoc /> */
        public void Deploy(ServiceConfiguration configuration)
        {
            try
            {
                _services.DeployAsync(configuration).Wait();
            }
            catch (AggregateException ex)
            {
                throw ex.InnerException ?? ex;
            }
        }

        /** <inheritDoc /> */
        public Task DeployAsync(ServiceConfiguration configuration)
        {
            return _services.DeployAsync(configuration);
        }

        /** <inheritDoc /> */
        public void DeployAll(IEnumerable<ServiceConfiguration> configurations)
        {
            try
            {
                _services.DeployAllAsync(configurations).Wait();
            }
            catch (AggregateException ex)
            {
                throw ex.InnerException ?? ex;
            }
        }

        /** <inheritDoc /> */
        public Task DeployAllAsync(IEnumerable<ServiceConfiguration> configurations)
        {
            return _services.DeployAllAsync(configurations);
        }

        /** <inheritDoc /> */
        public void Cancel(string name)
        {
            _services.CancelAsync(name).Wait();
        }

        /** <inheritDoc /> */
        public Task CancelAsync(string name)
        {
            return _services.CancelAsync(name);
        }

        /** <inheritDoc /> */
        public void CancelAll()
        {
            _services.CancelAllAsync().Wait();
        }

        /** <inheritDoc /> */
        public Task CancelAllAsync()
        {
            return _services.CancelAllAsync();
        }

        /** <inheritDoc /> */
        public ICollection<IServiceDescriptor> GetServiceDescriptors()
        {
            return _services.GetServiceDescriptors();
        }

        /** <inheritDoc /> */
        public T GetService<T>(string name)
        {
            return _services.GetService<T>(name);
        }

        /** <inheritDoc /> */
        public ICollection<T> GetServices<T>(string name)
        {
            return _services.GetServices<T>(name);
        }

        /** <inheritDoc /> */
        public T GetServiceProxy<T>(string name) where T : class
        {
            return _services.GetServiceProxy<T>(name);
        }

        /** <inheritDoc /> */
        public T GetServiceProxy<T>(string name, bool sticky) where T : class
        {
            return _services.GetServiceProxy<T>(name, sticky);
        }

        /** <inheritDoc /> */
        public dynamic GetDynamicServiceProxy(string name)
        {
            return _services.GetDynamicServiceProxy(name);
        }

        /** <inheritDoc /> */
        public dynamic GetDynamicServiceProxy(string name, bool sticky)
        {
            return _services.GetDynamicServiceProxy(name, sticky);
        }

        /** <inheritDoc /> */
        public IServices WithKeepBinary()
        {
            return new ServicesAsyncWrapper(_services.WithKeepBinary());
        }

        /** <inheritDoc /> */
        public IServices WithServerKeepBinary()
        {
            return new ServicesAsyncWrapper(_services.WithServerKeepBinary());
        }
    }
}