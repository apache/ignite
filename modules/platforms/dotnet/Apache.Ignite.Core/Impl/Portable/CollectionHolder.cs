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
    using System;
    using System.Diagnostics;
    using System.Linq;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Portable;

    /// <summary>
    /// Wrapper for .Net-specific collections.
    /// Can be interpreted as a Portable Object in Java, but not as a collection.
    /// </summary>
    internal class CollectionHolder : IPortableWriteAware
    {
        /** StringHashCode(assemblyQualifiedName) -> Type map. */
        private static readonly CopyOnWriteConcurrentDictionary<int, Type> TypeNameMap =
            new CopyOnWriteConcurrentDictionary<int, Type>();

        /** Collection. */
        private readonly object _collection;

        /** Write action. */
        private readonly Action<PortableWriterImpl, object> _writeAction;

        /// <summary>
        /// Initializes a new instance of the <see cref="CollectionHolder" /> class.
        /// </summary>
        public CollectionHolder(object collection, Action<PortableWriterImpl, object> writeAction)
        {
            Debug.Assert(collection != null);
            Debug.Assert(writeAction != null);

            _collection = collection;
            _writeAction = writeAction;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CollectionHolder"/> class.
        /// </summary>
        public CollectionHolder(IPortableReader reader)
        {
            var r = (PortableReaderImpl) reader.GetRawReader();

            var colType = ReadType(r);

            var colInfo = PortableCollectionInfo.GetInstance(colType);

            _collection = colInfo.ReadGeneric(r);
        }

        /// <summary>
        /// Gets the wrapped collection.
        /// </summary>
        public object Collection
        {
            get { return _collection; }
        }

        /** <inheritdoc /> */
        public void WritePortable(IPortableWriter writer)
        {
            Debug.Assert(_writeAction != null);

            var w = (PortableWriterImpl)writer.GetRawWriter();

            WriteType(_collection.GetType(), w);

            _writeAction(w, _collection);
        }

        /// <summary>
        /// Writes a Type to the writer.
        /// </summary>
        /// <param name="type">Type.</param>
        /// <param name="writer">Writer.</param>
        private static void WriteType(Type type, PortableWriterImpl writer)
        {
            Debug.Assert(type != null);
            Debug.Assert(writer != null);

            type = ReplaceTypesRecursive(type, typeof(IPortableBuilder), typeof(IPortableObject));
            type = ReplaceTypesRecursive(type, typeof(PortableUserObject), typeof(object));

            var desc = writer.Marshaller.GetDescriptor(type);

            if (desc != null)
            {
                writer.WriteBoolean(true);  // known type

                writer.WriteBoolean(desc.UserType);
                writer.WriteInt(desc.TypeId);
            }
            else
            {
                writer.WriteBoolean(false);  // unknown type

                var typeName = type.AssemblyQualifiedName;

                writer.WriteInt(PortableUtils.GetStringHashCode(typeName));
                writer.WriteString(typeName);
            }
        }

        /// <summary>
        /// Replaces type with another type in a generic of any depth.
        /// </summary>
        private static Type ReplaceTypesRecursive(Type type, Type target, Type replacement)
        {
            if (type == target)
                return replacement;

            if (!type.IsGenericType)
                return type;

            var def = type.GetGenericTypeDefinition();

            var args = type.GetGenericArguments().Select(x => ReplaceTypesRecursive(x, target, replacement)).ToArray();

            return def.MakeGenericType(args);
        }

        /// <summary>
        /// Reads a Type from a reader.
        /// </summary>
        /// <param name="reader">Reader.</param>
        /// <returns>Type.</returns>
        private static Type ReadType(PortableReaderImpl reader)
        {
            Debug.Assert(reader != null);

            var hasDesc = reader.ReadBoolean();

            if (hasDesc)
            {
                var userType = reader.ReadBoolean();
                var typeId = reader.ReadInt();

                var desc = reader.Marshaller.GetDescriptor(userType, typeId);

                return desc.Type;
            }

            var typeNameHash = reader.ReadInt();
            var typeName = reader.ReadString();

            return TypeNameMap.GetOrAdd(typeNameHash, x => Type.GetType(typeName, true));
        }
    }
}
