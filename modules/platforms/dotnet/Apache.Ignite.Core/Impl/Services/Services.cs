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

namespace Apache.Ignite.Core.Impl.Services
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Reflection;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Unmanaged;
    using Apache.Ignite.Core.Services;
    using UU = Apache.Ignite.Core.Impl.Unmanaged.UnmanagedUtils;

    /// <summary>
    /// Services implementation.
    /// </summary>
    internal sealed class Services : PlatformTarget, IServices
    {
        /** */
        private const int OpDeploy = 1;
        
        /** */
        private const int OpDeployMultiple = 2;

        /** */
        private const int OpDotnetServices = 3;

        /** */
        private const int OpInvokeMethod = 4;

        /** */
        private const int OpDescriptors = 5;

        /** */
        private readonly IClusterGroup _clusterGroup;

        /** Invoker binary flag. */
        private readonly bool _keepBinary;

        /** Server binary flag. */
        private readonly bool _srvKeepBinary;

        /** Async instance. */
        private readonly Lazy<Services> _asyncInstance;

        /// <summary>
        /// Initializes a new instance of the <see cref="Services" /> class.
        /// </summary>
        /// <param name="target">Target.</param>
        /// <param name="marsh">Marshaller.</param>
        /// <param name="clusterGroup">Cluster group.</param>
        /// <param name="keepBinary">Invoker binary flag.</param>
        /// <param name="srvKeepBinary">Server binary flag.</param>
        public Services(IUnmanagedTarget target, Marshaller marsh, IClusterGroup clusterGroup, 
            bool keepBinary, bool srvKeepBinary)
            : base(target, marsh)
        {
            Debug.Assert(clusterGroup  != null);

            _clusterGroup = clusterGroup;
            _keepBinary = keepBinary;
            _srvKeepBinary = srvKeepBinary;

            _asyncInstance = new Lazy<Services>(() => new Services(this));
        }

        /// <summary>
        /// Initializes a new async instance.
        /// </summary>
        /// <param name="services">The services.</param>
        private Services(Services services) : base(UU.ServicesWithAsync(services.Target), services.Marshaller)
        {
            _clusterGroup = services.ClusterGroup;
            _keepBinary = services._keepBinary;
            _srvKeepBinary = services._srvKeepBinary;
        }

        /** <inheritDoc /> */
        public IServices WithKeepBinary()
        {
            if (_keepBinary)
                return this;

            return new Services(Target, Marshaller, _clusterGroup, true, _srvKeepBinary);
        }

        /** <inheritDoc /> */
        public IServices WithServerKeepBinary()
        {
            if (_srvKeepBinary)
                return this;

            return new Services(UU.ServicesWithServerKeepBinary(Target), Marshaller, _clusterGroup, _keepBinary, true);
        }

        /** <inheritDoc /> */
        public IClusterGroup ClusterGroup
        {
            get { return _clusterGroup; }
        }

        /// <summary>
        /// Gets the asynchronous instance.
        /// </summary>
        private Services AsyncInstance
        {
            get { return _asyncInstance.Value; }
        }

        /** <inheritDoc /> */
        public void DeployClusterSingleton(string name, IService service)
        {
            IgniteArgumentCheck.NotNullOrEmpty(name, "name");
            IgniteArgumentCheck.NotNull(service, "service");

            DeployMultiple(name, service, 1, 1);
        }

        /** <inheritDoc /> */
        public Task DeployClusterSingletonAsync(string name, IService service)
        {
            AsyncInstance.DeployClusterSingleton(name, service);

            return AsyncInstance.GetTask();
        }

        /** <inheritDoc /> */
        public void DeployNodeSingleton(string name, IService service)
        {
            IgniteArgumentCheck.NotNullOrEmpty(name, "name");
            IgniteArgumentCheck.NotNull(service, "service");

            DeployMultiple(name, service, 0, 1);
        }

        /** <inheritDoc /> */
        public Task DeployNodeSingletonAsync(string name, IService service)
        {
            AsyncInstance.DeployNodeSingleton(name, service);

            return AsyncInstance.GetTask();
        }

        /** <inheritDoc /> */
        public void DeployKeyAffinitySingleton<TK>(string name, IService service, string cacheName, TK affinityKey)
        {
            IgniteArgumentCheck.NotNullOrEmpty(name, "name");
            IgniteArgumentCheck.NotNull(service, "service");
            IgniteArgumentCheck.NotNull(affinityKey, "affinityKey");

            Deploy(new ServiceConfiguration
            {
                Name = name,
                Service = service,
                CacheName = cacheName,
                AffinityKey = affinityKey,
                TotalCount = 1,
                MaxPerNodeCount = 1
            });
        }

        /** <inheritDoc /> */
        public Task DeployKeyAffinitySingletonAsync<TK>(string name, IService service, string cacheName, TK affinityKey)
        {
            AsyncInstance.DeployKeyAffinitySingleton(name, service, cacheName, affinityKey);

            return AsyncInstance.GetTask();
        }

        /** <inheritDoc /> */
        public void DeployMultiple(string name, IService service, int totalCount, int maxPerNodeCount)
        {
            IgniteArgumentCheck.NotNullOrEmpty(name, "name");
            IgniteArgumentCheck.NotNull(service, "service");

            DoOutOp(OpDeployMultiple, w =>
            {
                w.WriteString(name);
                w.WriteObject(service);
                w.WriteInt(totalCount);
                w.WriteInt(maxPerNodeCount);
            });
        }

        /** <inheritDoc /> */
        public Task DeployMultipleAsync(string name, IService service, int totalCount, int maxPerNodeCount)
        {
            AsyncInstance.DeployMultiple(name, service, totalCount, maxPerNodeCount);

            return AsyncInstance.GetTask();
        }

        /** <inheritDoc /> */
        public void Deploy(ServiceConfiguration configuration)
        {
            IgniteArgumentCheck.NotNull(configuration, "configuration");

            DoOutOp(OpDeploy, w =>
            {
                w.WriteString(configuration.Name);
                w.WriteObject(configuration.Service);
                w.WriteInt(configuration.TotalCount);
                w.WriteInt(configuration.MaxPerNodeCount);
                w.WriteString(configuration.CacheName);
                w.WriteObject(configuration.AffinityKey);

                if (configuration.NodeFilter != null)
                    w.WriteObject(configuration.NodeFilter);
                else
                    w.WriteObject<object>(null);
            });
        }

        /** <inheritDoc /> */
        public Task DeployAsync(ServiceConfiguration configuration)
        {
            AsyncInstance.Deploy(configuration);

            return AsyncInstance.GetTask();
        }

        /** <inheritDoc /> */
        public void Cancel(string name)
        {
            IgniteArgumentCheck.NotNullOrEmpty(name, "name");

            UU.ServicesCancel(Target, name);
        }

        /** <inheritDoc /> */
        public Task CancelAsync(string name)
        {
            AsyncInstance.Cancel(name);

            return AsyncInstance.GetTask();
        }

        /** <inheritDoc /> */
        public void CancelAll()
        {
            UU.ServicesCancelAll(Target);
        }

        /** <inheritDoc /> */
        public Task CancelAllAsync()
        {
            AsyncInstance.CancelAll();

            return AsyncInstance.GetTask();
        }

        /** <inheritDoc /> */
        public ICollection<IServiceDescriptor> GetServiceDescriptors()
        {
            return DoInOp(OpDescriptors, stream =>
            {
                var reader = Marshaller.StartUnmarshal(stream, _keepBinary);

                var size = reader.ReadInt();

                var result = new List<IServiceDescriptor>(size);

                for (var i = 0; i < size; i++)
                {
                    var name = reader.ReadString();

                    result.Add(new ServiceDescriptor(name, reader, this));
                }

                return result;
            });
        }

        /** <inheritDoc /> */
        public T GetService<T>(string name)
        {
            IgniteArgumentCheck.NotNullOrEmpty(name, "name");

            var services = GetServices<T>(name);

            if (services == null)
                return default(T);

            return services.FirstOrDefault();
        }

        /** <inheritDoc /> */
        public ICollection<T> GetServices<T>(string name)
        {
            IgniteArgumentCheck.NotNullOrEmpty(name, "name");

            return DoOutInOp<ICollection<T>>(OpDotnetServices, w => w.WriteString(name),
                r =>
                {
                    bool hasVal = r.ReadBool();

                    if (!hasVal)
                        return new T[0];

                    var count = r.ReadInt();
                        
                    var res = new List<T>(count);

                    for (var i = 0; i < count; i++)
                        res.Add(Marshaller.Ignite.HandleRegistry.Get<T>(r.ReadLong()));

                    return res;
                });
        }

        /** <inheritDoc /> */
        public T GetServiceProxy<T>(string name) where T : class
        {
            return GetServiceProxy<T>(name, false);
        }

        /** <inheritDoc /> */
        public T GetServiceProxy<T>(string name, bool sticky) where T : class
        {
            IgniteArgumentCheck.NotNullOrEmpty(name, "name");
            IgniteArgumentCheck.Ensure(typeof(T).IsInterface, "T", "Service proxy type should be an interface: " + typeof(T));

            // In local scenario try to return service instance itself instead of a proxy
            // Get as object because proxy interface may be different from real interface
            var locInst = GetService<object>(name) as T;

            if (locInst != null)
                return locInst;

            var javaProxy = UU.ServicesGetServiceProxy(Target, name, sticky);
            var platform = GetServiceDescriptors().Cast<ServiceDescriptor>().Single(x => x.Name == name).Platform;

            return new ServiceProxy<T>((method, args) =>
                InvokeProxyMethod(javaProxy, method, args, platform)).GetTransparentProxy();
        }

        /// <summary>
        /// Invokes the service proxy method.
        /// </summary>
        /// <param name="proxy">Unmanaged proxy.</param>
        /// <param name="method">Method to invoke.</param>
        /// <param name="args">Arguments.</param>
        /// <param name="platform">The platform.</param>
        /// <returns>
        /// Invocation result.
        /// </returns>
        private unsafe object InvokeProxyMethod(IUnmanagedTarget proxy, MethodBase method, object[] args, 
            Platform platform)
        {
            return DoOutInOp(OpInvokeMethod,
                writer => ServiceProxySerializer.WriteProxyMethod(writer, method, args, platform),
                stream => ServiceProxySerializer.ReadInvocationResult(stream, Marshaller, _keepBinary), proxy.Target);
        }
    }
}
