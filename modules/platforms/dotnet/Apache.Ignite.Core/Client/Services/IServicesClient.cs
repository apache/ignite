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

namespace Apache.Ignite.Core.Client.Services
{
    using System.Collections.Generic;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Services;

    /// <summary>
    /// Ignite distributed services client.
    /// </summary>
    public interface IServicesClient
    {
        /// <summary>
        /// Gets the cluster group for this <see cref="IServicesClient"/> instance.
        /// </summary>
        IClientClusterGroup ClusterGroup { get; }

        /// <summary>
        /// Gets a proxy for the service with the specified name.
        /// <para />
        /// Note: service proxies are not "sticky" - there is no guarantee that all calls will be made to the same
        /// remote service instance.
        /// </summary>
        /// <typeparam name="T">Service type.</typeparam>
        /// <param name="serviceName">Service name.</param>
        /// <returns>Proxy object that forwards all member calls to a remote Ignite service.</returns>
        T GetServiceProxy<T>(string serviceName) where T : class;

        /// <summary>
        /// Gets a proxy for the service with the specified name and caller context.
        /// <para />
        /// Note: service proxies are not "sticky" - there is no guarantee that all calls will be made to the same
        /// remote service instance.
        /// </summary>
        /// <typeparam name="T">Service type.</typeparam>
        /// <param name="serviceName">Service name.</param>
        /// <param name="callCtx">Service call context.</param>
        /// <returns>Proxy object that forwards all member calls to a remote Ignite service.</returns>
        [IgniteExperimental]
        T GetServiceProxy<T>(string serviceName, IServiceCallContext callCtx) where T : class;

        /// <summary>
        /// Gets metadata about all deployed services in the grid.
        /// </summary>
        /// <returns>Metadata about all deployed services in the grid.</returns>
        ICollection<IClientServiceDescriptor> GetServiceDescriptors();

        /// <summary>
        /// Gets metadata about service deployed in the grid.
        /// </summary>
        /// <param name="serviceName">Service name.</param>
        /// <returns>Metadata about all deployed services in the grid.</returns>
        IClientServiceDescriptor GetServiceDescriptor(string serviceName);

        /// <summary>
        /// Returns an instance with binary mode enabled.
        /// Service method results will be kept in binary form.
        /// </summary>
        /// <returns>Instance with binary mode enabled.</returns>
        IServicesClient WithKeepBinary();

        /// <summary>
        /// Returns an instance with server-side binary mode enabled.
        /// Service method arguments will be kept in binary form.
        /// </summary>
        /// <returns>Instance with server-side binary mode enabled.</returns>
        IServicesClient WithServerKeepBinary();
    }
}
