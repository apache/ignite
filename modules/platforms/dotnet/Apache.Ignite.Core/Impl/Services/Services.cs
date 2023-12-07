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
    using System.Collections;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Reflection;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Platform;
    using Apache.Ignite.Core.Services;

    /// <summary>
    /// Services implementation.
    /// </summary>
    internal sealed class Services : PlatformTargetAdapter, IServices
    {
        /*
         * Please keep the following constants in sync with
         * \modules\core\src\main\java\org\apache\ignite\internal\processors\platform\services\PlatformServices.java
         */

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
        private const int OpWithServerKeepBinary = 7;

        /** */
        private const int OpServiceProxy = 8;

        /** */
        private const int OpCancel = 9;

        /** */
        private const int OpCancelAll = 10;

        /** */
        private const int OpDeployAsync = 11;

        /** */
        private const int OpDeployMultipleAsync = 12;

        /** */
        private const int OpCancelAsync = 13;

        /** */
        private const int OpCancelAllAsync = 14;

        /** */
        private const int OpDeployAll = 15;

        /** */
        private const int OpDeployAllAsync = 16;

        /** */
        private readonly IClusterGroup _clusterGroup;

        /** Invoker binary flag. */
        private readonly bool _keepBinary;

        /** Server binary flag. */
        private readonly bool _srvKeepBinary;

        /// <summary>
        /// Initializes a new instance of the <see cref="Services" /> class.
        /// </summary>
        /// <param name="target">Target.</param>
        /// <param name="clusterGroup">Cluster group.</param>
        /// <param name="keepBinary">Invoker binary flag.</param>
        /// <param name="srvKeepBinary">Server binary flag.</param>
        public Services(IPlatformTargetInternal target, IClusterGroup clusterGroup, 
            bool keepBinary, bool srvKeepBinary)
            : base(target)
        {
            Debug.Assert(clusterGroup  != null);

            _clusterGroup = clusterGroup;
            _keepBinary = keepBinary;
            _srvKeepBinary = srvKeepBinary;
        }

        /** <inheritDoc /> */
        public IServices WithKeepBinary()
        {
            if (_keepBinary)
                return this;

            return new Services(Target, _clusterGroup, true, _srvKeepBinary);
        }

        /** <inheritDoc /> */
        public IServices WithServerKeepBinary()
        {
            if (_srvKeepBinary)
                return this;

            return new Services(DoOutOpObject(OpWithServerKeepBinary), _clusterGroup, _keepBinary, true);
        }

        /** <inheritDoc /> */
        public IClusterGroup ClusterGroup
        {
            get { return _clusterGroup; }
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
            IgniteArgumentCheck.NotNullOrEmpty(name, "name");
            IgniteArgumentCheck.NotNull(service, "service");

            return DeployMultipleAsync(name, service, 1, 1);
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
            IgniteArgumentCheck.NotNullOrEmpty(name, "name");
            IgniteArgumentCheck.NotNull(service, "service");

            return DeployMultipleAsync(name, service, 0, 1);
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
            IgniteArgumentCheck.NotNullOrEmpty(name, "name");
            IgniteArgumentCheck.NotNull(service, "service");
            IgniteArgumentCheck.NotNull(affinityKey, "affinityKey");

            return DeployAsync(new ServiceConfiguration
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
        public void DeployMultiple(string name, IService service, int totalCount, int maxPerNodeCount)
        {
            IgniteArgumentCheck.NotNullOrEmpty(name, "name");
            IgniteArgumentCheck.NotNull(service, "service");

            DoOutInOp(OpDeployMultiple, w =>
            {
                w.WriteString(name);
                w.WriteObject(service);
                w.WriteInt(totalCount);
                w.WriteInt(maxPerNodeCount);
            }, ReadDeploymentResult);
        }

        /** <inheritDoc /> */
        public Task DeployMultipleAsync(string name, IService service, int totalCount, int maxPerNodeCount)
        {
            IgniteArgumentCheck.NotNullOrEmpty(name, "name");
            IgniteArgumentCheck.NotNull(service, "service");

            return DoOutOpAsync(OpDeployMultipleAsync, w =>
            {
                w.WriteString(name);
                w.WriteObject(service);
                w.WriteInt(totalCount);
                w.WriteInt(maxPerNodeCount);
            }, _keepBinary, ReadDeploymentResult);
        }

        /** <inheritDoc /> */
        public void Deploy(ServiceConfiguration configuration)
        {
            ValidateConfiguration(configuration, "configuration");

            DoOutInOp(OpDeploy, w => configuration.Write(w), ReadDeploymentResult);
        }

        /** <inheritDoc /> */
        public Task DeployAsync(ServiceConfiguration configuration)
        {
            ValidateConfiguration(configuration, "configuration");

            return DoOutOpAsync(OpDeployAsync, w => configuration.Write(w), 
                _keepBinary, ReadDeploymentResult);
        }

        /** <inheritDoc /> */
        public void DeployAll(IEnumerable<ServiceConfiguration> configurations)
        {
            IgniteArgumentCheck.NotNull(configurations, "configurations");

            DoOutInOp(OpDeployAll, w => SerializeConfigurations(configurations, w), ReadDeploymentResult);
        }

        /** <inheritDoc /> */
        public Task DeployAllAsync(IEnumerable<ServiceConfiguration> configurations)
        {
            IgniteArgumentCheck.NotNull(configurations, "configurations");
 
            return DoOutOpAsync(OpDeployAllAsync, w => SerializeConfigurations(configurations, w),
                _keepBinary, ReadDeploymentResult);
        }

        /** <inheritDoc /> */
        public void Cancel(string name)
        {
            IgniteArgumentCheck.NotNullOrEmpty(name, "name");

            DoOutOp(OpCancel, w => w.WriteString(name));
        }

        /** <inheritDoc /> */
        public Task CancelAsync(string name)
        {
            IgniteArgumentCheck.NotNullOrEmpty(name, "name");

            return DoOutOpAsync(OpCancelAsync, w => w.WriteString(name));
        }

        /** <inheritDoc /> */
        public void CancelAll()
        {
            DoOutInOp(OpCancelAll);
        }

        /** <inheritDoc /> */
        public Task CancelAllAsync()
        {
            return DoOutOpAsync(OpCancelAllAsync);
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
                        res.Add((T)Marshaller.Ignite.HandleRegistry.Get<ServiceContext>(r.ReadLong()).Service);

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
            return GetServiceProxy<T>(name, sticky, null);
        }

        /** <inheritDoc /> */
        public T GetServiceProxy<T>(string name, bool sticky, IServiceCallContext callCtx) where T : class
        {
            IgniteArgumentCheck.NotNullOrEmpty(name, "name");
            IgniteArgumentCheck.Ensure(typeof(T).IsInterface, "T", 
                "Service proxy type should be an interface: " + typeof(T));

            var javaProxy = DoOutOpObject(OpServiceProxy, w =>
            {
                w.WriteString(name);
                w.WriteBoolean(sticky);
            });

            var platform = GetServiceDescriptors().Cast<ServiceDescriptor>().Single(x => x.Name == name).PlatformType;
            var callAttrs = GetCallerContextAttributes(callCtx);

            return ServiceProxyFactory<T>.CreateProxy((method, args) =>
                InvokeProxyMethod(javaProxy, method.Name, method, args, platform, callAttrs));
        }

        /** <inheritDoc /> */
        public dynamic GetDynamicServiceProxy(string name)
        {
            return GetDynamicServiceProxy(name, false);
        }

        /** <inheritDoc /> */
        public dynamic GetDynamicServiceProxy(string name, bool sticky)
        {
            return GetDynamicServiceProxy(name, sticky, null);
        }

        /** <inheritDoc /> */
        public dynamic GetDynamicServiceProxy(string name, bool sticky, IServiceCallContext callCtx)
        {
            IgniteArgumentCheck.NotNullOrEmpty(name, "name");

            var javaProxy = DoOutOpObject(OpServiceProxy, w =>
            {
                w.WriteString(name);
                w.WriteBoolean(sticky);
            });

            var platform = GetServiceDescriptors().Cast<ServiceDescriptor>().Single(x => x.Name == name).PlatformType;
            var callAttrs = GetCallerContextAttributes(callCtx);

            return new DynamicServiceProxy((methodName, args) =>
                InvokeProxyMethod(javaProxy, methodName, null, args, platform, callAttrs));
        }

        /// <summary>
        /// Invokes the service proxy method.
        /// </summary>
        /// <param name="proxy">Unmanaged proxy.</param>
        /// <param name="methodName">Name of the method.</param>
        /// <param name="method">Method to invoke.</param>
        /// <param name="args">Arguments.</param>
        /// <param name="platformType">The platform.</param>
        /// <param name="callAttrs">Service call context attributes.</param>
        /// <returns>
        /// Invocation result.
        /// </returns>
        private object InvokeProxyMethod(IPlatformTargetInternal proxy, string methodName,
            MethodBase method, object[] args, PlatformType platformType, IDictionary callAttrs)
        {
            bool locRegisterSameJavaType = Marshaller.RegisterSameJavaTypeTl.Value;

            if (platformType == PlatformType.Java)
            {
                Marshaller.RegisterSameJavaTypeTl.Value = true;
            }

            try
            {
                return DoOutInOp(OpInvokeMethod,
                    writer => ServiceProxySerializer.WriteProxyMethod(writer, methodName, method, args, platformType, callAttrs),
                    (stream, res) => ServiceProxySerializer.ReadInvocationResult(stream, Marshaller, _keepBinary),
                    proxy);
            }
            finally
            {
                if (platformType == PlatformType.Java)
                {
                    Marshaller.RegisterSameJavaTypeTl.Value = locRegisterSameJavaType;
                }
            }

        }

        /// <summary>
        /// Reads the deployment result.
        /// </summary>
        private object ReadDeploymentResult(BinaryReader r)
        {
            return r != null ? ReadDeploymentResult(r.Stream) : null;
        }

        /// <summary>
        /// Reads the deployment result.
        /// </summary>
        private object ReadDeploymentResult(IBinaryStream s)
        {
            ServiceProxySerializer.ReadDeploymentResult(s, Marshaller, _keepBinary);
            return null;
        }

        /// <summary>
        /// Gets the attributes of the service call context.
        /// </summary>
        /// <param name="callCtx">Service call context.</param>
        /// <returns>Service call context attributes.</returns>
        private IDictionary GetCallerContextAttributes(IServiceCallContext callCtx)
        {
            IgniteArgumentCheck.Ensure(callCtx == null || callCtx is ServiceCallContext, "callCtx", 
                "custom implementation of " + typeof(ServiceCallContext).Name + " is not supported." +
                " Please use " + typeof(ServiceCallContextBuilder).Name + " to create it.");

            return callCtx == null ? null : ((ServiceCallContext) callCtx).Values();
        }

        /// <summary>
        /// Performs ServiceConfiguration validation.
        /// </summary>
        /// <param name="configuration">Service configuration</param>
        /// <param name="argName">argument name</param>
        private static void ValidateConfiguration(ServiceConfiguration configuration, string argName)
        {
            IgniteArgumentCheck.NotNull(configuration, argName);
            IgniteArgumentCheck.NotNullOrEmpty(configuration.Name, string.Format("{0}.Name", argName));
            IgniteArgumentCheck.NotNull(configuration.Service, string.Format("{0}.Service", argName));

            if (configuration.Interceptors != null)
            {
                foreach (var interceptor in configuration.Interceptors)
                    IgniteArgumentCheck.NotNull(interceptor, string.Format("{0}.Interceptors[]", argName));
            }

        }

        /// <summary>
        /// Writes a collection of service configurations using passed BinaryWriter
        /// Also it performs basic validation of each service configuration and could throw exceptions
        /// </summary>
        /// <param name="configurations">a collection of service configurations </param>
        /// <param name="writer">Binary Writer</param>
        private static void SerializeConfigurations(IEnumerable<ServiceConfiguration> configurations, 
            BinaryWriter writer)
        {
            var pos = writer.Stream.Position;
            writer.WriteInt(0);  // Reserve count.

            var cnt = 0;

            foreach (var cfg in configurations)
            {
                ValidateConfiguration(cfg, string.Format("configurations[{0}]", cnt));
                cfg.Write(writer);
                cnt++;
            }

            IgniteArgumentCheck.Ensure(cnt > 0, "configurations", "empty collection");

            writer.Stream.WriteInt(pos, cnt);
        }
    }
}
