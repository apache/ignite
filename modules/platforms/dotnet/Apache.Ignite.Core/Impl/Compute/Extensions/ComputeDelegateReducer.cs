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
    internal class ComputeDelegateReducer<R1, R2> : IComputeReducer<R1, R2>
    {
        /** */
        private readonly Func<R1, bool> collect;

        /** */
        private readonly Func<IList<R1>, R2> reduce;

        /** */
        private readonly List<R1> results = new List<R1>();

        /// <summary>
        /// Initializes a new instance of the <see cref="ComputeDelegateReducer{R1, R2}"/> class.
        /// </summary>
        /// <param name="collect">Collect.</param>
        /// <param name="reduce">Reduce.</param>
        public ComputeDelegateReducer(Func<R1, bool> collect, Func<IList<R1>,  R2> reduce)
        {
            this.collect = collect;
            this.reduce = reduce;
        }

        /** <inheritdoc /> */
        public bool Collect(R1 res)
        {
            results.Add(res);

            return collect(res);
        }

        /** <inheritdoc /> */
        public R2 Reduce()
        {
            return reduce(results);
        }
    }
}