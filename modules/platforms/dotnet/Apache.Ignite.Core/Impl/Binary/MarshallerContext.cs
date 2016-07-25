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
    using System.Diagnostics;
    using Apache.Ignite.Core.Impl.Unmanaged;

    /// <summary>
    /// Marshaller context, manages dynamic type registration in the marshaller cache.
    /// </summary>
    internal class MarshallerContext
    {
        /** */
        private readonly Ignite _ignite;

        /// <summary>
        /// Initializes a new instance of the <see cref="MarshallerContext"/> class.
        /// </summary>
        /// <param name="ignite">The ignite.</param>
        public MarshallerContext(Ignite ignite)
        {
            Debug.Assert(ignite != null);

            _ignite = ignite;
        }

        /// <summary>
        /// Registers the type.
        /// </summary>
        /// <param name="id">The identifier.</param>
        /// <param name="type">The type.</param>
        /// <returns>True if registration succeeded; otherwise, false.</returns>
        public bool RegisterType(int id, Type type)
        {
            Debug.Assert(type != null);
            Debug.Assert(id != BinaryUtils.TypeUnregistered);

            return UnmanagedUtils.ProcessorRegisterType(_ignite.InteropProcessor, id, type.AssemblyQualifiedName);
        }

        /// <summary>
        /// Gets the type by id.
        /// </summary>
        /// <param name="id">The identifier.</param>
        /// <returns>Type or null.</returns>
        public Type GetType(int id)
        {
            var name = UnmanagedUtils.ProcessorGetClass(_ignite.InteropProcessor, id);

            return name == null ? null : Type.GetType(name, true);
        }
    }
}
