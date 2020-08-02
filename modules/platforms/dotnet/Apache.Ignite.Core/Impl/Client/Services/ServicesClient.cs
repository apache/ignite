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
    using Apache.Ignite.Core.Client.Services;

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
        public T GetServiceProxy<T>(string name) where T : class
        {
            // TODO: Can we support async invocation with proxies?
            throw new System.NotImplementedException();
        }

        private T InvokeService<T>(string name, params object[] args)
        {
            // TODO: Can we handle the types properly here?
            return _ignite.Socket.DoOutInOp(
                ClientOp.ServiceInvoke,
                ctx =>
                {

                },
                ctx => ctx.Reader.ReadObject<T>());
        }
    }
}
