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

namespace Apache.Ignite.Core.Tests.Plugin.Cache
{
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Plugin.Cache;

    /// <summary>
    /// Configuration with a plugin with Java part.
    /// </summary>
    public class CacheJavaPluginConfiguration : ICachePluginConfiguration
    {
        /// <summary>
        /// Gets or sets the custom property.
        /// </summary>
        public string Foo { get; set; }

        /** <inheritdoc /> */
        public int? CachePluginConfigurationClosureFactoryId
        {
            get { return 0; }
        }

        /** <inheritdoc /> */
        public void WriteBinary(IBinaryRawWriter writer)
        {
            writer.WriteString(Foo);
        }
    }
}