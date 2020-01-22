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

namespace Apache.Ignite.Core.Impl.Client
{
    using System;

    /// <summary>
    /// Version attribute for <see cref="ClientOp"/>.
    /// </summary>
    [AttributeUsage(AttributeTargets.Field)]
    internal sealed class MinVersionAttribute : Attribute
    {
        /** */
        private readonly ClientProtocolVersion _version;

        /// <summary>
        /// Initializes a new instance of <see cref="MinVersionAttribute"/> class.
        /// </summary>
        public MinVersionAttribute(short major, short minor, short maintenance)
        {
            _version = new ClientProtocolVersion(major, minor, maintenance);
        }

        /// <summary>
        /// Gets the version.
        /// </summary>
        public ClientProtocolVersion Version
        {
            get { return _version; }
        }
    }
}