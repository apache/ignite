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

namespace Apache.Ignite.Core.Cache.Affinity
{
    using System.Diagnostics.CodeAnalysis;
    using Apache.Ignite.Core.Cache.Affinity.Fair;
    using Apache.Ignite.Core.Cache.Affinity.Rendezvous;

    /// <summary>
    /// Represents a function that maps cache keys to cluster nodes.
    /// <para />
    /// Predefined implementations: 
    /// <see cref="RendezvousAffinityFunction"/>, <see cref="FairAffinityFunction"/>.
    /// </summary>
    public interface IAffinityFunction
    {
        /// <summary>
        ///  Resets cache affinity to its initial state. This method will be called by the system any time 
        /// the affinity has been sent to remote node where it has to be reinitialized.
        /// If your implementation of affinity function has no initialization logic, leave this method empty.
        /// </summary>
        void Reset();
    }
}