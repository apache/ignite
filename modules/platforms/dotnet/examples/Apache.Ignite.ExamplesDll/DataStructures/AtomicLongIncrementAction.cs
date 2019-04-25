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

namespace Apache.Ignite.ExamplesDll.DataStructures
{
    using System;
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.Compute;
    using Apache.Ignite.Core.DataStructures;
    using Apache.Ignite.Core.Resource;

    /// <summary>
    /// Increments atomic long.
    /// </summary>
    [Serializable]
    public class AtomicLongIncrementAction : IComputeAction
    {
        /** */
        public const string AtomicLongName = "dotnet_atomic_long";

        /** */
        [InstanceResource] private readonly IIgnite _ignite;

        /// <summary>
        /// Invokes action.
        /// </summary>
        public void Invoke()
        {
            IAtomicLong atomicLong = _ignite.GetAtomicLong(AtomicLongName, 0, true);

            for (int i = 0; i < 20; i++)
                Console.WriteLine(">>> AtomicLong value has been incremented: " + atomicLong.Increment());
        }
    }
}
