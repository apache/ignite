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

namespace Apache.Ignite.Core.Impl
{
    using System.Diagnostics;

    /// <summary>
    /// Ignite distributed re-entrant lock.
    /// </summary>
    internal class IgniteLock : PlatformTargetAdapter, IIgniteLock
    {
        /** */
        private readonly string _name;

        /// <summary>
        /// Initializes a new instance of <see cref="IgniteLock"/>.
        /// </summary>
        /// <param name="target">Target.</param>
        /// <param name="name">Name.</param>
        public IgniteLock(IPlatformTargetInternal target, string name) : base(target)
        {
            Debug.Assert(!string.IsNullOrEmpty(name));

            _name = name;
        }

        public string Name
        {
            get { return _name; }
        }
    }
}