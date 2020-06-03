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
    using System;
    using System.Threading;
    using Apache.Ignite.Core.Configuration;

    /// <summary>
    /// Distributed re-entrant monitor (lock).
    /// <para />
    /// The functionality is similar to the standard <see cref="Monitor"/> class, but works across all cluster nodes.
    /// <para />
    /// This API corresponds to <c>IgniteLock</c> in Java.
    /// </summary>
    public interface IIgniteLock : IDisposable
    {
        /// <summary>
        /// Gets the lock configuration.
        /// </summary>
        LockConfiguration Configuration { get; }

        void Enter(); // TODO: Rename to Enter, TryEnter - like CacheLock, Monitor, etc?

        bool TryEnter();
        
        bool TryEnter(TimeSpan timeout);

        void Exit();

        bool IsBroken();
        
        bool IsEntered();
        
        // TODO: IgniteCondition is like Monitor.Wait / Pulse
        // TODO: Understand why getOrCreateCondition has name arg - what does this achieve? Ask on dev list?
    }
}