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

    /// <summary>
    /// Ignite distributed re-entrant lock.
    /// </summary>
    internal class IgniteMonitor : PlatformTargetAdapter, IIgniteMonitor
    {
        /// <summary>
        /// Lock operations.
        /// </summary>
        private enum Op
        {
            Lock = 1,
            TryLock = 2,
            Unlock = 3,
            Close = 4,
            IsBroken = 5,
            IsLocked = 6
        }
        
        /** */
        private readonly string _name;

        /** */
        private readonly bool _failoverSafe;
        
        /** */
        private readonly bool _fair;

        /// <summary>
        /// Initializes a new instance of <see cref="IgniteMonitor"/>.
        /// </summary>
        /// <param name="target">Target.</param>
        /// <param name="name">Name.</param>
        /// <param name="failoverSafe">Failover-safe flag.</param>
        /// <param name="fair">Fair flag.</param>
        public IgniteMonitor(IPlatformTargetInternal target, string name, bool failoverSafe, bool fair) 
            : base(target)
        {
            Debug.Assert(!string.IsNullOrEmpty(name));

            _name = name;
            _failoverSafe = failoverSafe;
            _fair = fair;
        }
        
        /** <inheritDoc /> */
        public string Name
        {
            get { return _name; }
        }

        /** <inheritDoc /> */
        public bool IsFailoverSafe
        {
            get { return _failoverSafe; }
        }

        /** <inheritDoc /> */
        public bool IsFair
        {
            get { return _fair; }
        }

        /** <inheritDoc /> */
        public void Enter()
        {
            Target.InLongOutLong((int) Op.Lock, 0);
        }

        /** <inheritDoc /> */
        public bool TryEnter()
        {
            return Target.InLongOutLong((int) Op.TryLock, 0) == True;
        }

        public bool TryEnter(TimeSpan timeout)
        {
            throw new NotImplementedException();
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
        public void Dispose()
        {
            // TODO: Should we call Exit from here?
            // It throws an exception if not exited, but this does not make sense?
            Target.InLongOutLong((int) Op.Close, 0);
        }
    }
}