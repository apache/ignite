/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
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
        private readonly IServices services;

        /// <summary>
        /// Initializes a new instance of the <see cref="ServicesAsyncWrapper"/> class.
        /// </summary>
        /// <param name="services">Services to wrap.</param>
        public ServicesAsyncWrapper(IServices services)
        {
            this.services = services.WithAsync();
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
            get { return services.ClusterGroup; }
        }

        /** <inheritDoc /> */
        public void DeployClusterSingleton(string name, IService service)
        {
            services.DeployClusterSingleton(name, service);
            WaitResult();
        }

        /** <inheritDoc /> */
        public void DeployNodeSingleton(string name, IService service)
        {
            services.DeployNodeSingleton(name, service);
            WaitResult();
        }

        /** <inheritDoc /> */
        public void DeployKeyAffinitySingleton<K>(string name, IService service, string cacheName, K affinityKey)
        {
            services.DeployKeyAffinitySingleton(name, service, cacheName, affinityKey);
            WaitResult();
        }

        /** <inheritDoc /> */
        public void DeployMultiple(string name, IService service, int totalCount, int maxPerNodeCount)
        {
            services.DeployMultiple(name, service, totalCount, maxPerNodeCount);
            WaitResult();
        }

        /** <inheritDoc /> */
        public void Deploy(ServiceConfiguration configuration)
        {
            services.Deploy(configuration);
            WaitResult();
        }

        /** <inheritDoc /> */
        public void Cancel(string name)
        {
            services.Cancel(name);
            WaitResult();
        }

        /** <inheritDoc /> */
        public void CancelAll()
        {
            services.CancelAll();
            WaitResult();
        }

        /** <inheritDoc /> */
        public ICollection<IServiceDescriptor> GetServiceDescriptors()
        {
            return services.GetServiceDescriptors();
        }

        /** <inheritDoc /> */
        public T GetService<T>(string name)
        {
            return services.GetService<T>(name);
        }

        /** <inheritDoc /> */
        public ICollection<T> GetServices<T>(string name)
        {
            return services.GetServices<T>(name);
        }

        /** <inheritDoc /> */
        public T GetServiceProxy<T>(string name) where T : class
        {
            return services.GetServiceProxy<T>(name);
        }

        /** <inheritDoc /> */
        public T GetServiceProxy<T>(string name, bool sticky) where T : class
        {
            return services.GetServiceProxy<T>(name, sticky);
        }

        /** <inheritDoc /> */
        public IServices WithKeepPortable()
        {
            return new ServicesAsyncWrapper(services.WithKeepPortable());
        }

        /** <inheritDoc /> */
        public IServices WithServerKeepPortable()
        {
            return new ServicesAsyncWrapper(services.WithServerKeepPortable());
        }

        /// <summary>
        /// Waits for the async result.
        /// </summary>
        private void WaitResult()
        {
            services.GetFuture().Get();
        }
    }
}