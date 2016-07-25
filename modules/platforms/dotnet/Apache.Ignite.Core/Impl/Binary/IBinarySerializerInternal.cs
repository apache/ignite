﻿/*
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

namespace Apache.Ignite.Core.Impl.Binary
{
    using System;

    /// <summary>
    /// Internal generic serializer interface.
    /// </summary>
    internal interface IBinarySerializerInternal
    {
        /// <summary>
        /// Write binary object.
        /// </summary>
        void WriteBinary<T>(T obj, BinaryWriter writer);

        /// <summary>
        /// Read binary object.
        /// </summary>
        T ReadBinary<T>(BinaryReader reader, Type type, int pos);

        /// <summary>
        /// Gets a value indicating whether this serializer supports handles.
        /// </summary>
        bool SupportsHandles { get; }
    }
}
