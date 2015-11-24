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

namespace Apache.Ignite.Core.Cache.Store
{
    using System.Collections.Generic;

    /// <summary>
    /// Session for the cache store operations. The main purpose of cache store session
    /// is to hold context between multiple store invocations whenever in transaction. For example,
    /// you can save current database connection in the session <see cref="Properties"/> map. You can then
    /// commit this connection in the <see cref="ICacheStore.SessionEnd(bool)"/> method.
    /// </summary>
    public interface ICacheStoreSession
    {
        /// <summary>
        /// Cache name for the current store operation. Note that if the same store
        /// is reused between different caches, then the cache name will change between
        /// different store operations.
        /// </summary>
        string CacheName { get; }

        /// <summary>
        /// Current session properties. You can add properties directly to the returned map.
        /// </summary>
        IDictionary<object, object> Properties { get; }
    }
}
