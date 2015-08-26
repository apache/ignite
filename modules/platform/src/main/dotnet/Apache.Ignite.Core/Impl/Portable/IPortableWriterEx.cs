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

namespace Apache.Ignite.Core.Impl.Portable
{
    using System.Collections.Generic;
    using Apache.Ignite.Core.Impl.Portable.IO;
    using Apache.Ignite.Core.Portable;

    /// <summary>
    /// Writer interface for internal usage.
    /// </summary>
    internal interface IPortableWriterEx : IPortableWriter, IPortableRawWriter
    {
        /// <summary>
        /// Gets the marshaller.
        /// </summary>
        PortableMarshaller Marshaller { get; }

        /// <summary>
        /// Gets the stream.
        /// </summary>
        IPortableStream Stream { get; }

        /// <summary>
        /// Gets the metadata.
        /// </summary>
        IDictionary<int, IPortableMetadata> Metadata { get; }

        /// <summary>
        /// Enable detach mode for the next object.
        /// </summary>
        void DetachNext();

        /// <summary>
        /// Writes an object.
        /// </summary>
        /// <param name="obj">Object.</param>
        void Write<T>(T obj);

        /// <summary>
        /// Sets new builder.
        /// </summary>
        /// <param name="portableBuilder">Builder.</param>
        /// <returns>Previous builder.</returns>
        PortableBuilderImpl SetBuilder(PortableBuilderImpl portableBuilder);

        /// <summary>
        /// Saves metadata for this session.
        /// </summary>
        /// <param name="typeId">Type ID.</param>
        /// <param name="typeName">Type name.</param>
        /// <param name="affKeyFieldName">Affinity key field name.</param>
        /// <param name="fields">Fields metadata.</param>
        void SaveMetadata(int typeId, string typeName, string affKeyFieldName, IDictionary<string, int> fields);
    }
}