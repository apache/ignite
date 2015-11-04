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

namespace Apache.Ignite.Core.Impl.DataStructures
{
    using System.Diagnostics;
    using Apache.Ignite.Core.DataStructures;
    using Apache.Ignite.Core.Impl.Portable;
    using Apache.Ignite.Core.Impl.Unmanaged;

    /// <summary>
    /// Atomic reference.
    /// </summary>
    internal class AtomicReference<T> : PlatformTarget, IAtomicReference<T>
    {
        /** */
        private readonly string _name;

        /** <inheritDoc /> */
        public AtomicReference(IUnmanagedTarget target, PortableMarshaller marsh, string name)
            : base(target, marsh)
        {
            Debug.Assert(!string.IsNullOrEmpty(name));

            _name = name;
        }

        /** <inheritDoc /> */
        public string Name
        {
            get { return _name; }
        }

        /** <inheritDoc /> */
        public T Get()
        {
            throw new System.NotImplementedException();
        }

        /** <inheritDoc /> */
        public void Set(T value)
        {
            throw new System.NotImplementedException();
        }

        /** <inheritDoc /> */
        public T CompareExchange(T value, T comparand)
        {
            throw new System.NotImplementedException();
        }

        /** <inheritDoc /> */
        public bool IsClosed()
        {
            return UnmanagedUtils.AtomicReferenceIsClosed(Target);
        }

        /** <inheritDoc /> */
        public void Close()
        {
            UnmanagedUtils.AtomicReferenceClose(Target);
        }
    }
}
