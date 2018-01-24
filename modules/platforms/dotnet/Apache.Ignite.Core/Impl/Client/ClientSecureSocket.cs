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

namespace Apache.Ignite.Core.Impl.Client
{
    using System;
    using System.Diagnostics;
    using System.Net.Security;
    using System.Net.Sockets;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Impl.Binary.IO;

    /// <summary>
    /// Wrapper over framework socket for Ignite thin client operations, with SSL.
    /// </summary>
    internal class ClientSecureSocket : IClientSocket
    {
        /** Stream. */
        private readonly SslStream _stream;

        /// <summary>
        /// Initializes a new instance of the <see cref="ClientSecureSocket"/> class.
        /// </summary>
        /// <param name="clientConfiguration">The client configuration.</param>
        public ClientSecureSocket(IgniteClientConfiguration clientConfiguration)
        {
            Debug.Assert(clientConfiguration != null);
            Debug.Assert(clientConfiguration.SslStreamFactory != null);

            _stream = Connect(clientConfiguration);
        }

        /// <summary>
        /// Connects the SSL stream.
        /// </summary>
        private static SslStream Connect(IgniteClientConfiguration cfg)
        {
            var sock = ClientSocket.Connect(cfg);

            return cfg.SslStreamFactory.Create(new NetworkStream(sock));
        }

        /** <inheritDoc /> */
        public T DoOutInOp<T>(ClientOp opId, Action<IBinaryStream> writeAction, Func<IBinaryStream, T> readFunc,
            Func<ClientStatusCode, string, T> errorFunc = null)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public Task<T> DoOutInOpAsync<T>(ClientOp opId, Action<IBinaryStream> writeAction,
            Func<IBinaryStream, T> readFunc, Func<ClientStatusCode, string, T> errorFunc = null)
        {
            // TODO: Proper async.
            var tcs = new TaskCompletionSource<T>();

            tcs.SetResult(DoOutInOp(opId, writeAction, readFunc, errorFunc));

            return tcs.Task;
        }

        /** <inheritDoc /> */
        public void Dispose()
        {
            _stream.Dispose();
        }
    }
}
