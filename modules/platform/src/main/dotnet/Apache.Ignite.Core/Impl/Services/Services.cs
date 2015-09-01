/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace Apache.Ignite.Core.Impl.Services
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Reflection;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Portable;
    using Apache.Ignite.Core.Impl.Unmanaged;
    using Apache.Ignite.Core.Services;
    using A = Apache.Ignite.Core.Impl.Common.GridArgumentCheck;
    using UU = Apache.Ignite.Core.Impl.Unmanaged.UnmanagedUtils;

    /// <summary>
    /// Services implementation.
    /// </summary>
    internal class Services : GridTarget, IServices
    {
        /** */
        private const int OP_DEPLOY = 1;
        
        /** */
        private const int OP_DEPLOY_MULTIPLE = 2;

        /** */
        private const int OP_DOTNET_SERVICES = 3;

        /** */
        private const int OP_INVOKE_METHOD = 4;

        /** */
        private const int OP_DESCRIPTORS = 5;

        /** */
        protected readonly IClusterGroup clusterGroup;

        /** Invoker portable flag. */
        protected readonly bool keepPortable;

        /** Server portable flag. */
        protected readonly bool srvKeepPortable;

        /// <summary>
        /// Initializes a new instance of the <see cref="Services" /> class.
        /// </summary>
        /// <param name="target">Target.</param>
        /// <param name="marsh">Marshaller.</param>
        /// <param name="clusterGroup">Cluster group.</param>
        /// <param name="keepPortable">Invoker portable flag.</param>
        /// <param name="srvKeepPortable">Server portable flag.</param>
        public Services(IUnmanagedTarget target, PortableMarshaller marsh, IClusterGroup clusterGroup, 
            bool keepPortable, bool srvKeepPortable)
            : base(target, marsh)
        {
            Debug.Assert(clusterGroup  != null);

            this.clusterGroup = clusterGroup;
            this.keepPortable = keepPortable;
            this.srvKeepPortable = srvKeepPortable;
        }

        /** <inheritDoc /> */
        public virtual IServices WithKeepPortable()
        {
            if (keepPortable)
                return this;

            return new Services(target, marsh, clusterGroup, true, srvKeepPortable);
        }

        /** <inheritDoc /> */
        public virtual IServices WithServerKeepPortable()
        {
            if (srvKeepPortable)
                return this;

            return new Services(UU.ServicesWithServerKeepPortable(target), marsh, clusterGroup, keepPortable, true);
        }

        /** <inheritDoc /> */
        public virtual IServices WithAsync()
        {
            return new ServicesAsync(UU.ServicesWithAsync(target), marsh, clusterGroup, keepPortable, srvKeepPortable);
        }

        /** <inheritDoc /> */
        public virtual bool IsAsync
        {
            get { return false; }
        }

        /** <inheritDoc /> */
        public virtual IFuture GetFuture()
        {
            throw new InvalidOperationException("Asynchronous mode is disabled");
        }

        /** <inheritDoc /> */
        public virtual IFuture<TResult> GetFuture<TResult>()
        {
            throw new InvalidOperationException("Asynchronous mode is disabled");
        }

        /** <inheritDoc /> */
        public IClusterGroup ClusterGroup
        {
            get { return clusterGroup; }
        }

        /** <inheritDoc /> */
        public void DeployClusterSingleton(string name, IService service)
        {
            A.NotNullOrEmpty(name, "name");
            A.NotNull(service, "service");

            DeployMultiple(name, service, 1, 1);
        }

        /** <inheritDoc /> */
        public void DeployNodeSingleton(string name, IService service)
        {
            A.NotNullOrEmpty(name, "name");
            A.NotNull(service, "service");

            DeployMultiple(name, service, 0, 1);
        }

        /** <inheritDoc /> */
        public void DeployKeyAffinitySingleton<K>(string name, IService service, string cacheName, K affinityKey)
        {
            A.NotNullOrEmpty(name, "name");
            A.NotNull(service, "service");
            A.NotNull(affinityKey, "affinityKey");

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
        public void DeployMultiple(string name, IService service, int totalCount, int maxPerNodeCount)
        {
            A.NotNullOrEmpty(name, "name");
            A.NotNull(service, "service");

            DoOutOp(OP_DEPLOY_MULTIPLE, w =>
            {
                w.WriteString(name);
                w.WriteObject(service);
                w.WriteInt(totalCount);
                w.WriteInt(maxPerNodeCount);
            });
        }

        /** <inheritDoc /> */
        public void Deploy(ServiceConfiguration configuration)
        {
            A.NotNull(configuration, "configuration");

            DoOutOp(OP_DEPLOY, w =>
            {
                w.WriteString(configuration.Name);
                w.WriteObject(configuration.Service);
                w.WriteInt(configuration.TotalCount);
                w.WriteInt(configuration.MaxPerNodeCount);
                w.WriteString(configuration.CacheName);
                w.WriteObject(configuration.AffinityKey);

                if (configuration.NodeFilter != null)
                    w.WriteObject(new PortableOrSerializableObjectHolder(configuration.NodeFilter));
                else
                    w.WriteObject<PortableOrSerializableObjectHolder>(null);
            });
        }

        /** <inheritDoc /> */
        public void Cancel(string name)
        {
            A.NotNullOrEmpty(name, "name");

            UU.ServicesCancel(target, name);
        }

        /** <inheritDoc /> */
        public void CancelAll()
        {
            UU.ServicesCancelAll(target);
        }

        /** <inheritDoc /> */
        public ICollection<IServiceDescriptor> GetServiceDescriptors()
        {
            return DoInOp(OP_DESCRIPTORS, stream =>
            {
                var reader = marsh.StartUnmarshal(stream, keepPortable);

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
            A.NotNullOrEmpty(name, "name");

            var services = GetServices<T>(name);

            if (services == null)
                return default(T);

            return services.FirstOrDefault();
        }

        /** <inheritDoc /> */
        public ICollection<T> GetServices<T>(string name)
        {
            A.NotNullOrEmpty(name, "name");

            return DoOutInOp<ICollection<T>>(OP_DOTNET_SERVICES, w => w.WriteString(name),
                r =>
                {
                    bool hasVal = r.ReadBool();

                    if (hasVal)
                    {
                        var count = r.ReadInt();
                        
                        var res = new List<T>(count);

                        for (var i = 0; i < count; i++)
                            res.Add((T)marsh.Grid.HandleRegistry.Get<IService>(r.ReadLong()));

                        return res;
                    }
                    else
                        return null;
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
            A.NotNullOrEmpty(name, "name");
            A.Ensure(typeof(T).IsInterface, "T", "Service proxy type should be an interface: " + typeof(T));

            // In local scenario try to return service instance itself instead of a proxy
            // Get as object because proxy interface may be different from real interface
            var locInst = GetService<object>(name) as T;

            if (locInst != null)
                return locInst;

            var javaProxy = UU.ServicesGetServiceProxy(target, name, sticky);

            return new ServiceProxy<T>((method, args) => InvokeProxyMethod(javaProxy, method, args))
                .GetTransparentProxy();
        }

        /// <summary>
        /// Invokes the service proxy method.
        /// </summary>
        /// <param name="proxy">Unmanaged proxy.</param>
        /// <param name="method">Method to invoke.</param>
        /// <param name="args">Arguments.</param>
        /// <returns>
        /// Invocation result.
        /// </returns>
        private unsafe object InvokeProxyMethod(IUnmanagedTarget proxy, MethodBase method, object[] args)
        {
            return DoOutInOp(OP_INVOKE_METHOD,
                writer => ServiceProxySerializer.WriteProxyMethod(writer, method, args),
                stream => ServiceProxySerializer.ReadInvocationResult(stream, marsh, keepPortable), proxy.Target);
        }
    }
}
