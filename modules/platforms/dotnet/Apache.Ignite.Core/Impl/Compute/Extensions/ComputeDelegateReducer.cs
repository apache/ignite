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

namespace Apache.Ignite.Core.Impl.Compute.Extensions
{
    using System;
    using System.Collections.Generic;
    using Apache.Ignite.Core.Compute;

    /// <summary>
    /// Compute reducer from delegates.
    /// </summary>
    internal class ComputeDelegateReducer<TFuncRes, TRes> : IComputeReducer<TFuncRes, TRes>
    {
        /** */
        private readonly Func<TFuncRes, bool> _collect;

        /** */
        private readonly Func<IList<TFuncRes>, TRes> _reduce;

        /** */
        private readonly List<TFuncRes> _results = new List<TFuncRes>();

        /// <summary>
        /// Initializes a new instance of the <see cref="ComputeDelegateReducer{R1, R2}"/> class.
        /// </summary>
        /// <param name="collect">Collect.</param>
        /// <param name="reduce">Reduce.</param>
        public ComputeDelegateReducer(Func<TFuncRes, bool> collect, Func<IList<TFuncRes>,  TRes> reduce)
        {
            _collect = collect;
            _reduce = reduce;
        }

        /** <inheritdoc /> */
        public bool Collect(TFuncRes res)
        {
            _results.Add(res);

            return _collect(res);
        }

        /** <inheritdoc /> */
        public TRes Reduce()
        {
            return _reduce(_results);
        }
    }
}