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

namespace Apache.Ignite.Core.Impl.Common
{
    using System;
    using Apache.Ignite.Core.Impl.Binary.IO;

    /// <summary>
    /// Internal future interface.
    /// </summary>
    internal interface IFutureInternal
    {
        /// <summary>
        /// Set result from stream.
        /// </summary>
        /// <param name="stream">Stream.</param>
        void OnResult(IBinaryStream stream);

        /// <summary>
        /// Set null result.
        /// </summary>
        void OnNullResult();

        /// <summary>
        /// Set error result.
        /// </summary>
        /// <param name="err">Exception.</param>
        void OnError(Exception err);
    }
}