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
    public class AtomicReferenceModifyAction : IComputeAction
    {
        /** */
        public const string AtomicReferenceName = "dotnet_atomic_reference";

        /** */
        // ReSharper disable once UnassignedReadonlyField
        [InstanceResource] private readonly IIgnite _ignite;

        /// <summary>
        /// Invokes action.
        /// </summary>
        public void Invoke()
        {
            // Get or create the atomic reference.
            IAtomicReference<Guid> atomicRef = _ignite.GetAtomicReference(AtomicReferenceName, Guid.Empty, true);

            // Get local node id.
            Guid localNodeId = _ignite.GetCluster().GetLocalNode().Id;

            // Replace empty value with current node id.
            Guid expectedValue = Guid.Empty;

            Guid originalValue = atomicRef.CompareExchange(localNodeId, expectedValue);

            if (originalValue == expectedValue)
                Console.WriteLine(">>> Successfully updated atomic reference on node {0}", localNodeId);
            else
                Console.WriteLine(">>> Failed to update atomic reference on node {0}, actual value is {1}", 
                    localNodeId, originalValue);
        }
    }
}
