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

namespace Apache.Ignite.Core.Client
{
    using System.IO;
    using System.Net.Security;

    /// <summary>
    /// SSL Stream Factory defines how SSL connection is established.
    /// </summary>
    public interface ISslStreamFactory
    {
        /// <summary>
        /// Creates the SSL stream.
        /// </summary>
        /// <param name="stream">The underlying raw stream.</param>
        /// <param name="targetHost">Target host.</param>
        /// <returns>
        /// SSL stream.
        /// </returns>
        SslStream Create(Stream stream, string targetHost);
    }
}
