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

namespace Apache.Ignite.Core.Impl.Cache
{
    using System;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cache.Query;

    /// <summary>
    /// Extended Cache interface for internal needs.
    /// </summary>
    public interface ICacheInternal
    {
        /// <summary>
        /// Queries separate entry fields.
        /// </summary>
        /// <typeparam name="T">Type of the result.</typeparam>
        /// <param name="qry">SQL fields query.</param>
        /// <param name="readerFunc">Reader function, takes raw reader and field count, returns typed result.</param>
        /// <returns>
        /// Cursor.
        /// </returns>
        IQueryCursor<T> Query<T>(SqlFieldsQuery qry, Func<IBinaryRawReader, int, T> readerFunc);

        /// <summary>
        /// Invokes a cache extension.
        /// </summary>
        /// <typeparam name="T">The type of the result.</typeparam>
        /// <param name="extensionId">The extension identifier.</param>
        /// <param name="opCode">The extension op code.</param>
        /// <param name="writeAction">The write action.</param>
        /// <param name="readFunc">The read action.</param>
        /// <returns>
        /// Result of the processing.
        /// </returns>
        T DoOutInOpExtension<T>(int extensionId, int opCode, Action<IBinaryRawWriter> writeAction, 
            Func<IBinaryRawReader, T> readFunc);

        /// <summary>
        /// Gets the cache configuration.
        /// </summary>
        CacheConfiguration GetConfiguration();
    }
}
