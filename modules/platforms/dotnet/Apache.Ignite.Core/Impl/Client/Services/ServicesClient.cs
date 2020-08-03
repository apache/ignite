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
    using System.Diagnostics;
    using System.Reflection;
    using Apache.Ignite.Core.Client.Services;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Services;

    /// <summary>
    /// Services client.
    /// </summary>
    internal class ServicesClient : IServicesClient
    {
        /** */
        private readonly IgniteClient _ignite;

        /// <summary>
        /// Initializes a new instance of <see cref="ServicesClient"/> class.
        /// </summary>
        /// <param name="ignite">Ignite.</param>
        public ServicesClient(IgniteClient ignite)
        {
            Debug.Assert(ignite != null);

            _ignite = ignite;
        }

        /** <inheritdoc /> */
        public T GetServiceProxy<T>(string serviceName) where T : class
        {
            IgniteArgumentCheck.NotNullOrEmpty(serviceName, "name");

            return ServiceProxyFactory<T>.CreateProxy((method, args) => InvokeProxyMethod(serviceName, method, args));
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
                    w.WriteByte(0); // TODO: Flags - keepBinary, hasTypes
                    w.WriteLong(0); // TODO: Timeout
                    w.WriteInt(0); // TODO: Cluster nodes

                    w.WriteString(method.Name);

                    w.WriteInt(args.Length);
                    foreach (var arg in args)
                    {
                        w.WriteObjectDetached(arg);
                    }
                },
                ctx => ctx.Reader.ReadObject<object>());
        }
    }
}
