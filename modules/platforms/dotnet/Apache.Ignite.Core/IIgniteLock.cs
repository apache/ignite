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

namespace Apache.Ignite.Core
{
    /// <summary>
    /// Distributed re-entrant lock.
    /// </summary>
    public interface IIgniteLock
    {
        /// <summary>
        /// Gets the name of this lock.
        /// </summary>
        string Name { get; }
        
        /// <summary>
        /// Gets a value indicating whether this lock is failover-safe: when true, if any node leaves topology,
        /// all locks already acquired by that node are silently released and become available for other nodes
        /// to acquire. When false, all threads on other nodes waiting to acquire the lock are interrupted.
        /// </summary>
        bool FailoverSafe { get; }
        
        /// <summary>
        /// Gets a value indicating whether this lock is in fair mode.
        /// <para />
        /// When true, under contention, locks favor granting access to the longest-waiting thread.
        /// Otherwise this lock does not guarantee any particular access order.
        /// <para />
        /// Fair locks accessed by many threads may display lower overall throughput than those using the default
        /// setting, but have smaller variances in times to obtain locks and guarantee lack of starvation.
        /// </summary>
        bool Fair { get; }
    }
}