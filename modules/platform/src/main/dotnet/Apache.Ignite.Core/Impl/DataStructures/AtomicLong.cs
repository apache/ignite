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

    using UU = Apache.Ignite.Core.Impl.Unmanaged.UnmanagedUtils;

    /// <summary>
    /// Atomic long wrapper.
    /// </summary>
    internal sealed class AtomicLong : PlatformDisposableTarget, IAtomicLong
    {
        /** */
        private readonly string _name;

        /// <summary>
        /// Initializes a new instance of the <see cref="AtomicLong"/> class.
        /// </summary>
        /// <param name="target">The target.</param>
        /// <param name="marsh">The marshaller.</param>
        /// <param name="name">The name.</param>
        public AtomicLong(IUnmanagedTarget target, PortableMarshaller marsh, string name) : base(target, marsh)
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
        public long Read()
        {
            return UU.AtomicLongRead(Target);
        }

        /** <inheritDoc /> */
        public long Increment()
        {
            return UU.AtomicLongIncrement(Target);
        }

        /** <inheritDoc /> */
        public long Add(long value)
        {
            return UU.AtomicLongAdd(Target, value);
        }

        /** <inheritDoc /> */
        public long Decrement()
        {
            return UU.AtomicLongDecrement(Target);
        }

        /** <inheritDoc /> */
        public long Exchange(long value)
        {
            return UU.AtomicLongExchange(Target, value);
        }

        /** <inheritDoc /> */
        public long CompareExchange(long value, long comparand)
        {
            return UU.AtomicLongCompareExchange(Target, value, comparand);
        }

        /** <inheritDoc /> */
        public bool IsRemoved()
        {
            return UU.AtomicLongIsRemoved(Target);
        }

        /** <inheritDoc /> */
        ~AtomicLong()
        {
            Dispose(false);
        }

        /** <inheritDoc /> */
        protected override void Dispose(bool disposing)
        {
            UU.AtomicLongClose(Target);
            
            base.Dispose(disposing);
        }
    }
}