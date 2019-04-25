/*
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

namespace Apache.Ignite.Core.Impl.Cache
{
    using System;

    /// <summary>
    /// Internal cache locking interface.
    /// </summary>
    internal interface ICacheLockInternal
    {
        /// <summary>
        /// Enters the lock.
        /// </summary>
        void Enter(long id);

        /// <summary>
        /// Tries to enter the lock.
        /// </summary>
        bool TryEnter(long id, TimeSpan timeout);

        /// <summary>
        /// Exits the lock.
        /// </summary>
        void Exit(long id);

        /// <summary>
        /// Closes the lock.
        /// </summary>
        void Close(long id);
    }
}
