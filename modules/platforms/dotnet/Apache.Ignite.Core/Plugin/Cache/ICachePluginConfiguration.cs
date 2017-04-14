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

namespace Apache.Ignite.Core.Plugin.Cache
{
    using System.Diagnostics.CodeAnalysis;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache.Configuration;

    /// <summary>
    /// Cache plugin configuration marker interface. Starting point to extend <see cref="CacheConfiguration"/>
    /// and extend existing cache functionality.
    /// </summary>
    [SuppressMessage("Microsoft.Design", "CA1040:AvoidEmptyInterfaces")]
    public interface ICachePluginConfiguration
    {
        /// <summary>
        /// Gets the id to locate PlatformCachePluginConfigurationClosureFactory on Java side
        /// and read the data written by <see cref="WriteBinary"/> method.
        /// </summary>
        int? CachePluginConfigurationClosureFactoryId { get; }

        /// <summary>
        /// Writes this instance to a raw writer.
        /// This method will be called when <see cref="CachePluginConfigurationClosureFactoryId"/> 
        /// is not null to propagate configuration to the Java side.
        /// </summary>
        /// <param name="writer">The writer.</param>
        void WriteBinary(IBinaryRawWriter writer);
    }
}
