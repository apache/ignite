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
    using System.Diagnostics.CodeAnalysis;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Client.Cache;
    using Apache.Ignite.Core.Datastream;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Client.Cache;
    using Apache.Ignite.Core.Impl.Cluster;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Handle;
    using Apache.Ignite.Core.Impl.Plugin;

    /// <summary>
    /// Thin client implementation
    /// </summary>
    internal class IgniteClient : IIgniteInternal, IIgniteClient
    {
        /** Socket. */
        private readonly ClientSocket _socket;

        /** Marshaller. */
        private readonly Marshaller _marsh;

        /** Binary processor. */
        private readonly IBinaryProcessor _binProc;

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteClient"/> class.
        /// </summary>
        /// <param name="clientConfiguration">The client configuration.</param>
        public IgniteClient(IgniteClientConfiguration clientConfiguration)
        {
            Debug.Assert(clientConfiguration != null);

            _socket = new ClientSocket(clientConfiguration);

            _marsh = new Marshaller(clientConfiguration.BinaryConfiguration)
            {
                Ignite = this
            };

            _binProc = clientConfiguration.BinaryProcessor ?? new BinaryProcessorClient(_socket);
        }

        /// <summary>
        /// Gets the socket.
        /// </summary>
        public ClientSocket Socket
        {
            get { return _socket; }
        }

        /** <inheritDoc /> */
        [SuppressMessage("Microsoft.Usage", "CA1816:CallGCSuppressFinalizeCorrectly",
            Justification = "There is no finalizer.")]
        public void Dispose()
        {
            _socket.Dispose();
        }

        /** <inheritDoc /> */
        public ICacheClient<TK, TV> GetCache<TK, TV>(string name)
        {
            IgniteArgumentCheck.NotNull(name, "name");

            return new CacheClient<TK, TV>(this, name);
        }

        /** <inheritDoc /> */
        public IIgnite GetIgnite()
        {
            throw GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public IBinary GetBinary()
        {
            throw GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public IBinaryProcessor BinaryProcessor
        {
            get { return _binProc; }
        }

        /** <inheritDoc /> */
        public IgniteConfiguration Configuration
        {
            get { throw GetClientNotSupportedException(); }
        }

        /** <inheritDoc /> */
        public HandleRegistry HandleRegistry
        {
            get { throw GetClientNotSupportedException(); }
        }

        /** <inheritDoc /> */
        public ClusterNodeImpl GetNode(Guid? id)
        {
            throw GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public Marshaller Marshaller
        {
            get { return _marsh; }
        }

        /** <inheritDoc /> */
        public PluginProcessor PluginProcessor
        {
            get { throw GetClientNotSupportedException(); }
        }

        /** <inheritDoc /> */
        public IDataStreamer<TK, TV> GetDataStreamer<TK, TV>(string cacheName, bool keepBinary)
        {
            throw GetClientNotSupportedException();
        }

        /// <summary>
        /// Gets the client not supported exception.
        /// </summary>
        public static NotSupportedException GetClientNotSupportedException(string info = null)
        {
            var msg = "Operation is not supported in thin client mode.";

            if (info != null)
            {
                msg += " " + info;
            }

            return new NotSupportedException(msg);
        }
    }
}
