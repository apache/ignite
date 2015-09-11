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

namespace Apache.Ignite.Core.Services
{
    using System;

    /// <summary>
    /// Represents service execution context.
    /// </summary>
    public interface IServiceContext
    {
        /// <summary>
        /// Gets service name.
        /// </summary>
        /// <returns>
        /// Service name.
        /// </returns>
        string Name { get; }

        /// <summary>
        /// Gets service execution ID. Execution ID is guaranteed to be unique across all service deployments.
        /// </summary>
        /// <returns>
        /// Service execution ID.
        /// </returns>
        Guid ExecutionId { get; }

        /// <summary>
        /// Get flag indicating whether service has been cancelled or not.
        /// </summary>
        /// <returns>
        /// Flag indicating whether service has been cancelled or not.
        /// </returns>
        bool IsCancelled { get; }

        /// <summary>
        /// Gets cache name used for key-to-node affinity calculation. 
        /// This parameter is optional and is set only when key-affinity service was deployed.
        /// </summary>
        /// <returns>
        /// Cache name, possibly null.
        /// </returns>
        string CacheName { get; }

        /// <summary>
        /// Gets affinity key used for key-to-node affinity calculation. 
        /// This parameter is optional and is set only when key-affinity service was deployed.
        /// </summary>
        /// <value>
        /// Affinity key, possibly null.
        /// </value>
        object AffinityKey { get; }
    }
}