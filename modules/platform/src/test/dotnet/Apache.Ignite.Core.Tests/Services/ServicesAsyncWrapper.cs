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
    using System.Collections.Generic;
    using System.Diagnostics;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Common;
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
            _services = services.WithAsync();
        }

        /** <inheritDoc /> */
        public IServices WithAsync()
        {
            return this;
        }

        /** <inheritDoc /> */
        public bool IsAsync
        {
            get { return true; }
        }

        /** <inheritDoc /> */
        public IFuture GetFuture()
        {
            Debug.Fail("ServicesAsyncWrapper.Future() should not be called. It always returns null.");
            return null;
        }

        /** <inheritDoc /> */
        public IFuture<TResult> GetFuture<TResult>()
        {
            Debug.Fail("ServicesAsyncWrapper.Future() should not be called. It always returns null.");
            return null;
        }

        /** <inheritDoc /> */
        public IClusterGroup ClusterGroup
        {
            get { return _services.ClusterGroup; }
        }

        /** <inheritDoc /> */
        public void DeployClusterSingleton(string name, IService service)
        {
            _services.DeployClusterSingleton(name, service);
            WaitResult();
        }

        /** <inheritDoc /> */
        public void DeployNodeSingleton(string name, IService service)
        {
            _services.DeployNodeSingleton(name, service);
            WaitResult();
        }

        /** <inheritDoc /> */
        public void DeployKeyAffinitySingleton<TK>(string name, IService service, string cacheName, TK affinityKey)
        {
            _services.DeployKeyAffinitySingleton(name, service, cacheName, affinityKey);
            WaitResult();
        }

        /** <inheritDoc /> */
        public void DeployMultiple(string name, IService service, int totalCount, int maxPerNodeCount)
        {
            _services.DeployMultiple(name, service, totalCount, maxPerNodeCount);
            WaitResult();
        }

        /** <inheritDoc /> */
        public void Deploy(ServiceConfiguration configuration)
        {
            _services.Deploy(configuration);
            WaitResult();
        }

        /** <inheritDoc /> */
        public void Cancel(string name)
        {
            _services.Cancel(name);
            WaitResult();
        }

        /** <inheritDoc /> */
        public void CancelAll()
        {
            _services.CancelAll();
            WaitResult();
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
        public IServices WithKeepPortable()
        {
            return new ServicesAsyncWrapper(_services.WithKeepPortable());
        }

        /** <inheritDoc /> */
        public IServices WithServerKeepPortable()
        {
            return new ServicesAsyncWrapper(_services.WithServerKeepPortable());
        }

        /// <summary>
        /// Waits for the async result.
        /// </summary>
        private void WaitResult()
        {
            _services.GetFuture().Get();
        }
    }
}