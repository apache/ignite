﻿/*
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

namespace IgniteExamples.Shared.DataStructures
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
