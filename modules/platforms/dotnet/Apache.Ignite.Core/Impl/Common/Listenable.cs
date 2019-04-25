/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Core.Impl.Common
{
    /// <summary>
    /// Platform listenable.
    /// </summary>
    internal class Listenable : PlatformTargetAdapter
    {
        /** */
        private const int OpCancel = 1;

        /// <summary>
        /// Initializes a new instance of the <see cref="Listenable"/> class.
        /// </summary>
        /// <param name="target">Target.</param>
        public Listenable(IPlatformTargetInternal target) : base(target)
        {
            // No-op.
        }

        /// <summary>
        /// Cancels the listenable.
        /// </summary>
        public void Cancel()
        {
            DoOutInOp(OpCancel);
        }
    }
}
