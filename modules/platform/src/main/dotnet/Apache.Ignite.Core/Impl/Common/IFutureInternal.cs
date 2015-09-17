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

namespace Apache.Ignite.Core.Impl.Common
{
    using System;
    using Apache.Ignite.Core.Impl.Portable.IO;

    /// <summary>
    /// Internal future interface.
    /// </summary>
    [CLSCompliant(false)]
    public interface IFutureInternal
    {
        /// <summary>
        /// Set result from stream.
        /// </summary>
        /// <param name="stream">Stream.</param>
        void OnResult(IPortableStream stream);

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