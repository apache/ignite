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

namespace Apache.Ignite.Core.Impl
{
    using System;
    using System.Diagnostics;
    using Apache.Ignite.Core.Configuration;

    /// <summary>
    /// Ignite distributed reentrant lock.
    /// </summary>
    internal class IgniteLock : PlatformTargetAdapter, IIgniteLock
    {
        /// <summary>
        /// Lock operations.
        /// </summary>
        private enum Op
        {
            Lock = 1,
            TryLock = 2,
            Unlock = 3,
            Remove = 4,
            IsBroken = 5,
            IsLocked = 6,
            IsRemoved = 7
        }

        /** */
        private const long TimeoutInfinite = -1;

        /** */
        private readonly LockConfiguration _cfg;

        /// <summary>
        /// Initializes a new instance of <see cref="IgniteLock"/>.
        /// </summary>
        public IgniteLock(IPlatformTargetInternal target, LockConfiguration cfg)
            : base(target)
        {
            Debug.Assert(cfg != null);

            _cfg = cfg;
        }

        /** <inheritDoc /> */
        public LockConfiguration Configuration
        {
            get { return new LockConfiguration(_cfg); }
        }

        /** <inheritDoc /> */
        public void Enter()
        {
            Target.InLongOutLong((int) Op.Lock, 0);
        }

        /** <inheritDoc /> */
        public bool TryEnter()
        {
            return Target.InLongOutLong((int) Op.TryLock, TimeoutInfinite) == True;
        }

        /** <inheritDoc /> */
        public bool TryEnter(TimeSpan timeout)
        {
            return Target.InLongOutLong((int) Op.TryLock, (long) timeout.TotalMilliseconds) == True;
        }

        /** <inheritDoc /> */
        public void Exit()
        {
            Target.InLongOutLong((int) Op.Unlock, 0);
        }

        /** <inheritDoc /> */
        public bool IsBroken()
        {
            return Target.InLongOutLong((int) Op.IsBroken, 0) == True;
        }

        /** <inheritDoc /> */
        public bool IsEntered()
        {
            return Target.InLongOutLong((int) Op.IsLocked, 0) == True;
        }

        /** <inheritDoc /> */
        public void Remove()
        {
            Target.InLongOutLong((int) Op.Remove, 0);
        }

        /** <inheritDoc /> */
        public bool IsRemoved()
        {
            return Target.InLongOutLong((int) Op.IsRemoved, 0) == True;
        }
    }
}
