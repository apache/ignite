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

namespace Apache.Ignite.Core.Impl.Cache.Query
{
    using System;
    using System.Collections.Generic;
    using Apache.Ignite.Core.Impl.Binary;

    /// <summary>
    /// Base for platform cursors.
    /// </summary>
    internal abstract class PlatformQueryQursorBase<T> : QueryCursorBase<T>
    {
        /** */
        private readonly IPlatformTargetInternal _target;

        /** */
        private const int OpGetAll = 1;

        /** */
        private const int OpGetBatch = 2;

        /** */
        private const int OpIterator = 4;

        /** */
        private const int OpIteratorClose = 5;

        /// <summary>
        /// Initializes a new instance of the <see cref="PlatformQueryQursorBase{T}"/> class.
        /// </summary>
        /// <param name="target">The target.</param>
        /// <param name="keepBinary">Keep binary flag.</param>
        /// <param name="readFunc"></param>
        protected PlatformQueryQursorBase(IPlatformTargetInternal target, bool keepBinary, 
            Func<BinaryReader, T> readFunc) 
            : base(target.Marshaller, keepBinary, readFunc)
        {
            _target = target;
        }

        /// <summary>
        /// Gets the target.
        /// </summary>
        public IPlatformTargetInternal Target
        {
            get { return _target; }
        }

        /** <inheritdoc /> */
        protected override IList<T> GetAllInternal()
        {
            return _target.OutStream(OpGetAll, ConvertGetAll);
        }

        /** <inheritdoc /> */
        protected override void InitIterator()
        {
            _target.InLongOutLong(OpIterator, 0);
        }

        /** <inheritdoc /> */
        protected override T[] GetBatch()
        {
            return _target.OutStream(OpGetBatch, ConvertGetBatch);
        }

        /** <inheritdoc /> */
        protected override void Dispose(bool disposing)
        {
            try
            {
                _target.InLongOutLong(OpIteratorClose, 0);
            }
            finally
            {
                base.Dispose(disposing);
            }
        }
    }
}