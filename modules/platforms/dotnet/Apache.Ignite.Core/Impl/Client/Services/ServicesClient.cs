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
    using System.Diagnostics;
    using System.Reflection;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Client.Services;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Services;

    /// <summary>
    /// Services client.
    /// </summary>
    internal class ServicesClient : IServicesClient
    {
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
        }

        /** <inheritdoc /> */
        public IClientClusterGroup ClusterGroup
        {
            get { return _clusterGroup ?? _ignite.GetCluster(); }
        }

        /** <inheritdoc /> */
        public T GetServiceProxy<T>(string serviceName) where T : class
        {
            IgniteArgumentCheck.NotNullOrEmpty(serviceName, "name");

            return ServiceProxyFactory<T>.CreateProxy((method, args) => InvokeProxyMethod(serviceName, method, args));
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
        private object InvokeProxyMethod(string serviceName, MethodBase method, object[] args)
        {
            return _ignite.Socket.DoOutInOp(
                ClientOp.ServiceInvoke,
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

                    w.WriteInt(args.Length);
                    foreach (var arg in args)
                    {
                        w.WriteObjectDetached(arg);
                    }
                },
                ctx =>
                {
                    var reader = _keepBinary
                        ? ctx.Marshaller.StartUnmarshal(ctx.Stream, BinaryMode.ForceBinary)
                        : ctx.Reader;

                    return reader.ReadObject<object>();
                });
        }
    }
}
