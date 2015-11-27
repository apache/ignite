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
    /// Memory modes define whether cache entries are stored on heap memory, offheap memory, or in swap space.
    /// </summary>
    public enum CacheMemoryMode
    {
        /// <summary>
        /// Entries will be stored on-heap first.
        /// </summary>
        OnheapTiered,

        /// <summary>
        /// Entries will be stored off-heap in unmanaged memory.
        /// </summary>
        OffheapTiered,

        /// <summary>
        /// Entry keys will be stored on heap memory, and values will be stored in offheap memory.
        /// </summary>
        OffheapValues
    }
}