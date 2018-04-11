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

    /// <summary>
    /// Atomic long wrapper.
    /// </summary>
    internal sealed class AtomicSequence: PlatformTargetAdapter, IAtomicSequence
    {
        /** */
        private readonly string _name;

        /** */
        private enum Op
        {
            AddAndGet = 1,
            Close = 2,
            Get = 3,
            GetBatchSize = 6,
            IncrementAndGet = 7,
            IsClosed = 8,
            SetBatchSize = 9
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Apache.Ignite.Core.Impl.DataStructures.AtomicLong"/> class.
        /// </summary>
        /// <param name="target">The target.</param>
        /// <param name="name">The name.</param>
        public AtomicSequence(IPlatformTargetInternal target, string name)
            : base(target)
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
        public int BatchSize
        {
            get { return (int) DoOutInOp((int) Op.GetBatchSize); }
            set { DoOutInOp((int) Op.SetBatchSize, value); }
        }

        /** <inheritDoc /> */
        public bool IsClosed
        {
            get { return DoOutInOp((int) Op.IsClosed) == True; }
        }

        /** <inheritDoc /> */
        public void Close()
        {
            DoOutInOp((int) Op.Close);
        }
    }
}
