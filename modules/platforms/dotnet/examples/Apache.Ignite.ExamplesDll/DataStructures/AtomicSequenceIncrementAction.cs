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

namespace Apache.Ignite.ExamplesDll.DataStructures
{
    using System;
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.Compute;
    using Apache.Ignite.Core.DataStructures;
    using Apache.Ignite.Core.Resource;

    /// <summary>
    /// Increments atomic sequence.
    /// </summary>
    [Serializable]
    public class AtomicSequenceIncrementAction : IComputeAction
    {
        /** */
        public const string AtomicSequenceName = "dotnet_atomic_sequence";

        /** */
        [InstanceResource] private readonly IIgnite _ignite;

        /// <summary>
        /// Invokes action.
        /// </summary>
        public void Invoke()
        {
            IAtomicSequence atomicSequence = _ignite.GetAtomicSequence(AtomicSequenceName, 0, true);

            for (int i = 0; i < 20; i++)
                Console.WriteLine(">>> AtomicSequence value has been incremented: " + atomicSequence.Increment());
        }
    }
}
