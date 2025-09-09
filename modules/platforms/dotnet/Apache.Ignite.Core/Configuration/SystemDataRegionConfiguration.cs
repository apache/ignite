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
    using System.ComponentModel;
    using System.Diagnostics;
    using Apache.Ignite.Core.Binary;

    /// <summary>
    /// Data storage configuration for system cache.
    /// </summary>
    public class SystemDataRegionConfiguration
    {
        /// <summary>
        /// Default size in bytes of a memory chunk reserved for system cache initially.
        /// </summary>
        public const long DefaultInitialSize = 40 * 1024 * 1024;

        /// <summary>
        /// Default max size in bytes of a memory chunk for the system cache.
        /// </summary>
        public const long DefaultMaxSize = 100 * 1024 * 1024;

        /// <summary>
        /// Initializes a new instance of the <see cref="SystemDataRegionConfiguration"/> class.
        /// </summary>
        public SystemDataRegionConfiguration()
        {
            InitialSize = DefaultInitialSize;
            MaxSize = DefaultMaxSize;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DataStorageConfiguration"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        internal SystemDataRegionConfiguration(IBinaryRawReader reader)
        {
            Debug.Assert(reader != null);

            InitialSize = reader.ReadLong();
            MaxSize = reader.ReadLong();
        }
        
        /// <summary>
        /// Writes this instance to the specified writer.
        /// </summary>
        /// <param name="writer">The writer.</param>
        internal void Write(IBinaryRawWriter writer)
        {
           Debug.Assert(writer != null);
           
           writer.WriteLong(InitialSize);
           writer.WriteLong(MaxSize);
        }
        
        /// <summary>
        /// Gets or sets the size in bytes of a memory chunk reserved for system needs.
        /// </summary>
        [DefaultValue(DefaultInitialSize)]
        public long InitialSize { get; set; }
        
        /// <summary>
        /// Gets or sets the maximum memory region size in bytes reserved for system needs.
        /// </summary>
        [DefaultValue(DefaultMaxSize)]
        public long MaxSize { get; set; }
    }
}
