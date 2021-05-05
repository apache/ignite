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
    /// Defines async continuations behavior.
    /// </summary>
    public enum AsyncContinuationExecutor
    {
        /// <summary>
        /// Executes async continuations on the thread pool (default).
        /// </summary>
        ThreadPool = 0,

        /// <summary>
        /// Executes async continuations synchronously on the same thread that completes the previous operation.
        /// <para />
        /// WARNING: can cause deadlocks and performance issues when not used correctly.
        /// <para />
        /// Ignite performs cache operations using a special "striped" thread pool
        /// (see <see cref="IgniteConfiguration.StripedThreadPoolSize"/>). Using this synchronous mode means that
        /// async continuations (any code coming after <c>await cache.DoAsync()</c>, or code in <c>ContinueWith()</c>)
        /// will run on the striped pool:
        /// <ul>
        /// <li>
        /// Cache operations can't execute while user code runs on the striped thread.
        /// </li>
        /// <li>
        /// Attempting other cache operations on the striped thread can cause a deadlock.
        /// </li>
        /// </ul>
        /// <para />
        /// This mode can improve performance, because continuations do not have to be scheduled on another thread.
        /// However, special care is required to release striped threads as soon as possible.
        /// </summary>
        UnsafeSynchronous = 1,

        /// <summary>
        /// Indicates that custom executor is configured on the Java side.
        /// <para />
        /// This value should not be used explicitly.
        /// </summary>
        Custom = 2
    }
}
