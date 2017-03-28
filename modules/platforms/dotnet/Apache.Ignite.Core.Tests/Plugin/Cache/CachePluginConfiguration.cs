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

namespace Apache.Ignite.Core.Tests.Plugin.Cache
{
    using System;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Plugin.Cache;
    using NUnit.Framework;

    /// <summary>
    /// Cache plugin config.
    /// </summary>
    [Serializable]
    [CachePluginProviderType(typeof(CachePlugin))]
    public class CachePluginConfiguration : ICachePluginConfiguration
    {
        /// <summary>
        /// Gets or sets the test property.
        /// </summary>
        public string TestProperty { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether the plugin should throw an error.
        /// </summary>
        // ReSharper disable once UnusedAutoPropertyAccessor.Global
        public bool ThrowError { get; set; }

        /// <summary>
        /// Gets the id to locate PlatformCachePluginConfigurationClosureFactory on Java side
        /// and read the data written by
        /// <see cref="WriteBinary(IBinaryRawWriter)" /> method.
        /// </summary>
        public int? CachePluginConfigurationClosureFactoryId
        {
            get { return null; }
        }

        /// <summary>
        /// Writes this instance to a raw writer.
        /// This method will be called when <see cref="CachePluginConfigurationClosureFactoryId" />
        /// is not null to propagate configuration to the Java side.
        /// </summary>
        /// <param name="writer">The writer.</param>
        public void WriteBinary(IBinaryRawWriter writer)
        {
            Assert.Fail("Should not be called");
        }
    }
}