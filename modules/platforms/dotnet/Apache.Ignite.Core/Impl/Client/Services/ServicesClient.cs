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

namespace Apache.Ignite.Core.Impl.Client.Services
{
    using System;
    using System.Collections.Generic;
    using System.Collections;
    using System.Collections.Concurrent;
    using System.Diagnostics;
    using System.Linq;
    using System.Reflection;
    using System.Threading;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Client.Services;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Services;
    using Apache.Ignite.Core.Log;
    using Apache.Ignite.Core.Platform;
    using Apache.Ignite.Core.Services;

    /// <summary>
    /// Services client.
    /// </summary>
    internal class ServicesClient : IServicesClient
    {
        /** Max service topology update period. */
        internal static readonly TimeSpan SrvTopUpdatePeriod = TimeSpan.FromSeconds(60);
        
        /** */
        [Flags]
        private enum ServiceFlags : byte
        {
            KeepBinary = 1,

            // ReSharper disable once UnusedMember.Local
            HasParameterTypes = 2
        }

        /** */
        private readonly IgniteClient _ignite;

        /** */
        private readonly IClientClusterGroup _clusterGroup;

        /** */
        private readonly bool _keepBinary;

        /** */
        private readonly bool _serverKeepBinary;

        /** */
        private readonly TimeSpan _timeout;
        
        /** */
        private readonly ConcurrentDictionary<string, ServiceTopology> _servicesTopologies;

        /// <summary>
        /// Initializes a new instance of <see cref="ServicesClient"/> class.
        /// </summary>
        public ServicesClient(
            IgniteClient ignite,
            IClientClusterGroup clusterGroup = null,
            bool keepBinary = false,
            bool serverKeepBinary = false,
            TimeSpan timeout = default(TimeSpan))
        {
            Debug.Assert(ignite != null);

            _ignite = ignite;
            _clusterGroup = clusterGroup;
            _keepBinary = keepBinary;
            _serverKeepBinary = serverKeepBinary;
            _timeout = timeout;
            _servicesTopologies = ignite.GetConfiguration().EnablePartitionAwareness
                ? new ConcurrentDictionary<string, ServiceTopology>()
                : null;
        }

        /** <inheritdoc /> */
        public IClientClusterGroup ClusterGroup
        {
            get { return _clusterGroup ?? _ignite.GetCluster(); }
        }

        /** <inheritdoc /> */
        public T GetServiceProxy<T>(string serviceName) where T : class
        {
            return GetServiceProxy<T>(serviceName, null);
        }

        /** <inheritdoc /> */
        public T GetServiceProxy<T>(string serviceName, IServiceCallContext callCtx) where T : class
        {
            IgniteArgumentCheck.NotNullOrEmpty(serviceName, "name");
            IgniteArgumentCheck.Ensure(callCtx == null || callCtx is ServiceCallContext, "callCtx",
                "custom implementation of " + typeof(ServiceCallContext).Name + " is not supported." +
                " Please use " + typeof(ServiceCallContextBuilder).Name + " to create it.");

            var platformType = GetServiceDescriptor(serviceName).PlatformType;
            IDictionary callAttrs = callCtx == null ? null : ((ServiceCallContext) callCtx).Values();

            return ServiceProxyFactory<T>.CreateProxy(
                (method, args) => InvokeProxyMethod(serviceName, method, args, platformType, callAttrs)
            );
        }

        /** <inheritdoc /> */
        public ICollection<IClientServiceDescriptor> GetServiceDescriptors()
        {
            return _ignite.Socket.DoOutInOp(
                ClientOp.ServiceGetDescriptors,
                ctx => { },
                ctx =>
                {
                    var cnt = ctx.Reader.ReadInt();
                    var res = new List<IClientServiceDescriptor>(cnt);

                    for (var i = 0; i < cnt; i++)
                        res.Add(new ClientServiceDescriptor(ctx.Reader));

                    return res;
                });
        }

        /** <inheritdoc /> */
        public IClientServiceDescriptor GetServiceDescriptor(string serviceName)
        {
            return _ignite.Socket.DoOutInOp(
                ClientOp.ServiceGetDescriptor,
                ctx => ctx.Writer.WriteString(serviceName),
                ctx => new ClientServiceDescriptor(ctx.Reader));
        }

        /** <inheritdoc /> */
        public IServicesClient WithKeepBinary()
        {
            return new ServicesClient(_ignite, _clusterGroup, true, _serverKeepBinary, _timeout);
        }

        /** <inheritdoc /> */
        public IServicesClient WithServerKeepBinary()
        {
            return new ServicesClient(_ignite, _clusterGroup, _keepBinary, true, _timeout);
        }

        /// <summary>
        /// Invokes the proxy method.
        /// </summary>
        private object InvokeProxyMethod(string serviceName, MethodBase method, object[] args,
            PlatformType platformType, IDictionary callAttrs)
        {
            return _ignite.Socket.DoOutInOpOnNode(ClientOp.ServiceInvoke,
                ctx =>
                {
                    var w = ctx.Writer;

                    w.WriteString(serviceName);
                    w.WriteByte(_serverKeepBinary ? (byte) ServiceFlags.KeepBinary : (byte) 0);
                    w.WriteLong((long) _timeout.TotalMilliseconds);

                    if (_clusterGroup != null)
                    {
                        var nodes = _clusterGroup.GetNodes();
                        if (nodes.Count == 0)
                        {
                            throw new IgniteClientException("Cluster group is empty");
                        }

                        w.WriteInt(nodes.Count);

                        foreach (var node in nodes)
                        {
                            BinaryUtils.WriteGuid(node.Id, ctx.Stream);
                        }
                    }
                    else
                    {
                        w.WriteInt(0);
                    }

                    w.WriteString(method.Name);

                    ServiceProxySerializer.WriteMethodArguments(w, null, args, platformType);

                    if (ctx.Features.HasFeature(ClientBitmaskFeature.ServiceInvokeCtx))
                    {
                        w.WriteDictionary(callAttrs);
                    }
                    else if (callAttrs != null)
                    {
                        throw new IgniteClientException(
                            "Passing caller context to the service is not supported by the server");
                    }
                },
                ctx =>
                {
                    var reader = _keepBinary
                        ? ctx.Marshaller.StartUnmarshal(ctx.Stream, BinaryMode.ForceBinary)
                        : ctx.Reader;

                    return reader.ReadObject<object>();
                }, 
                GetServiceTopology(serviceName));
        }

        /// <summary>
        /// Provides actual known service topology or empty list if: partition awareness is not enabled,
        /// service topology is not supported or not received yet.
        /// </summary>
        private IList<Guid> GetServiceTopology(string serviceName)
        {
            if (_servicesTopologies == null || !_ignite.Socket.GetSocket().Features.HasFeature(ClientBitmaskFeature.ServiceTopology))
                return null;
            
            return _servicesTopologies.GetOrAdd(serviceName, s => new ServiceTopology(serviceName, this)).GetAndUpdate();
        }
        
        /// <summary>
        /// Keeps and process topology of certain service.
        /// </summary>
        private class ServiceTopology
        {
            /** Service name. */
            private readonly string _svcName;
            
            /** Ignite services. */
            private readonly ServicesClient _svcClient;

            /** Flag of topology update progress. */
            private int _updateInProgress;

            /** Time of the last update. */
            private long _lastUpdateRequestTimeTicks;

            /** Cluster topology version of the last update. */
            private long _lastAffTop;
            
            /** Ids of the nodes with at least one service instance. */
            private volatile IList<Guid> _nodes = new List<Guid>();

            /// <summary>
            /// Creates service topology holder.
            /// </summary>
            internal ServiceTopology(string name, ServicesClient svcClient)
            {
                _svcName = name;
                _svcClient = svcClient;
            }

            /// <summary>
            /// Asynchronously updates the topology.
            /// </summary>
            private async Task UpdateTopologyAsync()
            {
                if (Interlocked.CompareExchange(ref _updateInProgress, 1, 0) == 1)
                    return;

                var socket = _svcClient._ignite.Socket;

                var topVer = socket.GetTopologyVersion();

                var log = _svcClient._ignite.GetConfiguration().Logger;

                var groupNodes = _svcClient._clusterGroup?.GetNodes();
                
                var top = await socket.DoOutInOpAsync(ClientOp.ServiceGetTopology,
                    ctx => ctx.Writer.WriteString(_svcName),
                    ctx =>
                    {
                        var cnt = ctx.Reader.ReadInt();

                        var res = new List<Guid>(cnt);
                        
                        for (var i = 0; i < cnt; ++i)
                            res.Add(BinaryUtils.ReadGuid(ctx.Reader.Stream));

                        return res;
                    }, (status, err) =>
                    {
                        log.Error("Failed to update topology of the service '" + _svcName + "'.", err);

                        return _nodes;
                    }).ConfigureAwait(false);

                _nodes = FilterTopology(top, groupNodes?.Select(n => n.Id));
                
                Interlocked.Exchange(ref _lastUpdateRequestTimeTicks, DateTime.Now.Ticks);
                Interlocked.Exchange(ref _lastAffTop, topVer);

                if (log.IsEnabled(LogLevel.Debug))
                {
                    log.Debug("Topology of service '" + _svcName + "' has been updated. The " +
                              "service instance nodes: " + string.Join(", ", top.Select(gid => gid.ToString())) +
                              ". Effective topology with the cluster group is: " +
                              string.Join(", ", _nodes.Select(gid => gid.ToString())) + '.');
                }

                Interlocked.Exchange(ref _updateInProgress, 0);
            }

            /// <summary>
            /// Filters service topology regarding to the cluster group.
            /// </summary>
            private static IList<Guid> FilterTopology(IList<Guid> serviceTopology, IEnumerable<Guid> clusterGroup)
            {
                return clusterGroup == null ? serviceTopology : serviceTopology.Intersect(clusterGroup).ToList();
            }

            /// <summary>
            /// Provides last known service topology and asynchronously updates it if required.
            /// </summary>
            internal IList<Guid> GetAndUpdate()
            {
                if (Interlocked.CompareExchange(ref _updateInProgress, 0, 0) != 0)
                    return _nodes;

                var curAff = _svcClient._ignite.Socket.GetTopologyVersion();
                var lastKnownAff = Interlocked.Read(ref _lastAffTop);
                var sinceLastUpdate = TimeSpan.FromTicks(
                    DateTime.Now.Ticks - Interlocked.Read(ref _lastUpdateRequestTimeTicks));

                if (curAff > lastKnownAff || sinceLastUpdate > SrvTopUpdatePeriod)
                    _ = UpdateTopologyAsync().ConfigureAwait(false);

                return _nodes;
            }
        }
    }
}
