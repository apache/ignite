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

namespace Apache.Ignite.Core.Impl.Compute
{
    using System.Collections.Generic;
    using Apache.Ignite.Core.Compute;
    using Binary;

    /// <summary>
    /// Implements <see cref="IComputeTaskSession"/> by delegating the implementation to the Java side.
    /// </summary>
    internal class ComputeTaskSession : PlatformTargetAdapter, IComputeTaskSession
    {
        /// <summary>
        /// Operation codes
        /// </summary>
        private enum Op
        {
            GetAttribute = 1,
            SetAttributes = 2
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ComputeTaskSession"/> class.
        /// </summary>
        public ComputeTaskSession(IPlatformTargetInternal target) : base(target)
        {
        }

        /// <inheritdoc />
        public TV GetAttribute<TK, TV>(TK key) =>
            DoOutInOp<TV>((int) Op.GetAttribute, w => w.Write(key));

        /// <inheritdoc />
        public void SetAttributes<TK, TV>(params KeyValuePair<TK, TV>[] attrs) =>
            DoOutOp((int) Op.SetAttributes, writer => writer.WriteDictionary(attrs));
    }
}