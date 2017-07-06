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

namespace Apache.Ignite.Core.Plugin
{
    using Apache.Ignite.Core.Binary;

    /// <summary>
    /// Plugin configuration interface.
    /// <para />
    /// Implementations should be linked to corresponding <see cref="IPluginProvider{TConfig}"/>
    /// via <see cref="PluginProviderTypeAttribute"/>.
    /// <example>
    /// Example plugin implementation:
    /// <code>
    /// [PluginProviderType(typeof(MyPluginProvider))]
    /// class MyPluginConfig : IPluginConfiguration
    /// {
    ///     int CustomProperty { get; set; }
    /// }
    /// 
    /// class MyPluginProvider : IPluginProvider&lt;MyPluginConfig&gt;
    /// {
    ///     ...
    /// }
    /// </code>
    /// </example>
    /// </summary>
    public interface IPluginConfiguration
    {
        /// <summary>
        /// Gets the id to locate PlatformPluginConfigurationClosureFactory on Java side
        /// and read the data written by <see cref="WriteBinary"/> method.
        /// </summary>
        int? PluginConfigurationClosureFactoryId { get; }

        /// <summary>
        /// Writes this instance to a raw writer.
        /// This method will be called when <see cref="PluginConfigurationClosureFactoryId"/> is not null to propagate
        /// configuration to the Java side.
        /// </summary>
        /// <param name="writer">The writer.</param>
        void WriteBinary(IBinaryRawWriter writer);
    }
}