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

namespace Apache.Ignite.Core.Configuration
{
    /// <summary>
    /// <see cref="IIgniteLock"/> configuration.
    /// </summary>
    public class LockConfiguration
    {
        /// <summary>
        /// Initializes a new instance of <see cref="LockConfiguration"/> class.
        /// </summary>
        public LockConfiguration()
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of <see cref="LockConfiguration"/> class.
        /// </summary>
        /// <param name="other">Other configuration to copy.</param>
        public LockConfiguration(LockConfiguration other)
        {
            Name = other.Name;
            IsFair = other.IsFair;
            IsFailoverSafe = other.IsFailoverSafe;
        }

        /// <summary>
        /// Gets or sets the cluster-wide lock name.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether the lock should be in fair mode.
        /// <para />
        /// When true, under contention, locks favor granting access to the longest-waiting thread. Otherwise this
        /// lock does not guarantee any particular access order.
        /// <para />
        /// Fair locks accessed by many threads may display lower overall throughput than those using the default
        /// setting, but have smaller variances in times to obtain locks and guarantee lack of starvation.
        /// </summary>
        public bool IsFair { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether the lock should be failover-safe: when true,
        /// if any node leaves topology, all locks already acquired by that node are silently released
        /// and become available for other nodes to acquire. When false, all threads on other nodes
        /// waiting to acquire the lock are interrupted.
        /// </summary>
        public bool IsFailoverSafe { get; set; }
    }
}
