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
    /// Defines checkpoint pages order on disk.
    /// </summary>
    [Obsolete("Use Apache.Ignite.Core.Data.CheckpointWriteOrder")]
    public enum CheckpointWriteOrder
    {
        /// <summary>
        /// Pages are written in order provided by checkpoint pages collection iterator
        /// (which is basically a hashtable).
        /// </summary>
        Random,

        /// <summary>
        /// All checkpoint pages are collected into single list and sorted by page index.
        /// Provides almost sequential disk writes, which can be much faster on some SSD models.
        /// </summary>
        Sequential
    }
}
