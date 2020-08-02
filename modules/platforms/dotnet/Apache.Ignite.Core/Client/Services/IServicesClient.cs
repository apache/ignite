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
    using System.Threading.Tasks;

    /// <summary>
    /// Ignite distributed services client.
    /// </summary>
    public interface IServicesClient
    {
        /// <summary>
        /// Gets a proxy for the service with the specified name.
        /// </summary>
        /// <typeparam name="T">Service type.</typeparam>
        /// <param name="name">Service name.</param>
        /// <returns>Proxy object that forwards all member calls to a remote Ignite service.</returns>
        T GetServiceProxy<T>(string name) where T : class;

        // TODO: Do we need those?
        // * How do we make async requests with a proxy?
        // * Proxy objects are not available in .NET Standard, is this a problem for us?
        object InvokeService(string name, params object[] args);

        Task<object> InvokeServiceAsync(string name, params object[] args);
    }
}
