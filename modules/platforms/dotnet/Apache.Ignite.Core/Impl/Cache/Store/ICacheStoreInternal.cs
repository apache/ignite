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

namespace Apache.Ignite.Core.Impl.Cache.Store
{
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Binary.IO;

    /// <summary>
    /// Provides a non-generic way to work with <see cref="CacheStoreInternal{TK, TV}"/>.
    /// </summary>
    internal interface ICacheStoreInternal
    {
        /// <summary>
        /// Invokes a store operation.
        /// </summary>
        /// <param name="stream">Input stream.</param>
        /// <param name="grid">Grid.</param>
        /// <returns>Invocation result.</returns>
        /// <exception cref="IgniteException">Invalid operation type:  + opType</exception>
        int Invoke(IBinaryStream stream, Ignite grid);

        /// <summary>
        /// Initializes this instance with a grid.
        /// </summary>
        /// <param name="grid">Grid.</param>
        void Init(Ignite grid);
    }
}