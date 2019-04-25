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

namespace Apache.Ignite.Core.PersistentStore
{
    using System;

    /// <summary>
    /// Write Ahead Log mode.
    /// </summary>
    [Obsolete("Use Apache.Ignite.Core.Data.WalMode")]
    public enum WalMode
    {
        /// <summary>
        /// Default mode: full-sync disk writes. These writes survive power loss scenarios.
        /// </summary>
        Default,

        /// <summary>
        /// Log only mode: flushes application buffers. These writes survive process crash.
        /// </summary>
        LogOnly,

        /// <summary>
        /// Background mode. Does not force application buffer flush. Data may be lost in case of process crash.
        /// </summary>
        Background,

        /// <summary>
        /// WAL disabled.
        /// </summary>
        None
    }
}