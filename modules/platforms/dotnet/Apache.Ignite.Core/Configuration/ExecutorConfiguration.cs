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
    using Apache.Ignite.Core.Compute;

    /// <summary>
    /// Custom thread pool configuration for compute tasks.
    /// See <see cref="ICompute.WithExecutor"/>.
    /// </summary>
    public class ExecutorConfiguration
    {
        /** */
        private int? _size;

        /// <summary>
        /// Gets or sets the thread pool name.
        /// Can not be null and should be unique with respect to other custom executors.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Gets or sets the thread pool size.
        /// Defaults to <see cref="IgniteConfiguration.DefaultThreadPoolSize"/>
        /// </summary>
        public int Size
        {
            get { return _size ?? IgniteConfiguration.DefaultThreadPoolSize; }
            set { _size = value; }
        }
    }
}
