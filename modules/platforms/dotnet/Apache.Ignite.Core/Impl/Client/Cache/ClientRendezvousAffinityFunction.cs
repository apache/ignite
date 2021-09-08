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

namespace Apache.Ignite.Core.Impl.Client.Cache
{
    using System;
    using System.Diagnostics;

    /// <summary>
    /// Partial implementation of Rendezvous affinity function for Ignite thin client needs.
    /// </summary>
    internal static class ClientRendezvousAffinityFunction
    {
        /// <summary>
        /// Gets partition for given key.
        /// </summary>
        /// <param name="keyHash">Key hash code.</param>
        /// <param name="partitionCount">Partition count.</param>
        /// <returns>Partition number.</returns>
        public static int GetPartitionForKey(int keyHash, int partitionCount)
        {
            Debug.Assert(partitionCount > 0);

            var mask = (partitionCount & (partitionCount - 1)) == 0 ? partitionCount - 1 : -1;

            if (mask >= 0)
            {
                return (keyHash ^ (keyHash >> 16)) & mask;
            }

            var part = Math.Abs(keyHash % partitionCount);

            return part > 0 ? part : 0;
        }
    }
}
