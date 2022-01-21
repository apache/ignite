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
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Client.Services;

    /// <summary>
    /// Implementation of client service descriptor.
    /// </summary>
    internal class ClientServiceDescriptor : IClientServiceDescriptor
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ClientServiceDescriptor" /> class.
        /// </summary>
        /// <param name="reader">Reader.</param>
        public ClientServiceDescriptor(IBinaryRawReader reader)
        {
            if (reader == null)
                throw new ArgumentNullException(nameof(reader));

            Name = reader.ReadString();
            ServiceClass = reader.ReadString();
            TotalCount = reader.ReadInt();
            MaxPerNodeCount = reader.ReadInt();
            CacheName = reader.ReadString();
            OriginNodeId = reader.ReadGuid();
            PlatformType = (Platform.PlatformType) reader.ReadByte();
        }

        /** <inheritdoc /> */
        public string Name { get; private set; }

        /** <inheritdoc /> */
        public string ServiceClass { get; private set; }

        /** <inheritdoc /> */
        public int TotalCount { get; private set; }

        /** <inheritdoc /> */
        public int MaxPerNodeCount { get; private set; }

        /** <inheritdoc /> */
        public string CacheName { get; private set; }

        /** <inheritdoc /> */
        public Guid? OriginNodeId { get; private set; }

        /** <inheritdoc /> */
        public Platform.PlatformType PlatformType { get; private set; }
    }
}
