/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

namespace Apache.Ignite.Core.Impl.DataStructures
{
    using System.Diagnostics;
    using Apache.Ignite.Core.DataStructures;
    using Apache.Ignite.Core.Impl.Binary.IO;

    /// <summary>
    /// Atomic long wrapper.
    /// </summary>
    internal sealed class AtomicLong : PlatformTargetAdapter, IAtomicLong
    {
        /** */
        private readonly string _name;

        /** Operation codes. */
        private enum Op
        {
            AddAndGet = 1,
            Close = 2,
            CompareAndSetAndGet = 4,
            DecrementAndGet = 5,
            Get = 6,
            GetAndSet = 10,
            IncrementAndGet = 11,
            IsClosed = 12
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AtomicLong"/> class.
        /// </summary>
        /// <param name="target">The target.</param>
        /// <param name="name">The name.</param>
        public AtomicLong(IPlatformTargetInternal target, string name) : base(target)
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
            return DoOutInOp((int) Op.Get);
        }

        /** <inheritDoc /> */
        public long Increment()
        {
            return DoOutInOp((int) Op.IncrementAndGet);
        }

        /** <inheritDoc /> */
        public long Add(long value)
        {
            return DoOutInOp((int) Op.AddAndGet, value);
        }

        /** <inheritDoc /> */
        public long Decrement()
        {
            return DoOutInOp((int) Op.DecrementAndGet);
        }

        /** <inheritDoc /> */
        public long Exchange(long value)
        {
            return DoOutInOp((int) Op.GetAndSet, value);
        }

        /** <inheritDoc /> */
        public long CompareExchange(long value, long comparand)
        {
            return DoOutOp((int) Op.CompareAndSetAndGet, (IBinaryStream s) =>
            {
                s.WriteLong(comparand);
                s.WriteLong(value);
            });
        }

        /** <inheritDoc /> */
        public void Close()
        {
            DoOutInOp((int) Op.Close);
        }

        /** <inheritDoc /> */
        public bool IsClosed()
        {
            return DoOutInOp((int) Op.IsClosed) == True;
        }
    }
}