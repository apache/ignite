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

namespace Apache.Ignite.Core.Impl.Binary
{
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Client;

    /// <summary>
    /// Represents an object that can write itself to a raw binary writer using specific server version.
    /// </summary>
    internal interface IBinaryRawWriteAwareEx<in T> where T : IBinaryRawWriter
    {
        /// <summary>
        /// Writes this object to the given writer.
        /// </summary>
        /// <param name="writer">Writer.</param>
        /// <param name="srvVer">Server version.</param>
        /// <exception cref="System.IO.IOException">If write failed.</exception>
        void Write(T writer, ClientProtocolVersion srvVer);
    }

    /// <summary>
    /// Represents an object that can write itself to a raw binary writer.
    /// </summary>
    internal interface IBinaryRawWriteAwareEx : IBinaryRawWriteAwareEx<IBinaryRawWriter>
    {
        // No-op.
    }

    /// <summary>
    /// Represents an object that can write itself to a raw binary writer.
    /// </summary>
    internal interface IBinaryRawWriteAware<in T> where T : IBinaryRawWriter
    {
        /// <summary>
        /// Writes this object to the given writer.
        /// </summary> 
        /// <param name="writer">Writer.</param>
        /// <exception cref="System.IO.IOException">If write failed.</exception>
        void Write(T writer);
    }

    /// <summary>
    /// Represents an object that can write itself to a raw binary writer.
    /// </summary>
    internal interface IBinaryRawWriteAware : IBinaryRawWriteAware<IBinaryRawWriter>
    {
        // No-op.
    }
}
