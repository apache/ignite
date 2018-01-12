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

namespace Apache.Ignite.Core
{
    using System.Configuration;
    using System.Text;
    using System.Xml;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Ignite configuration section for app.config and web.config files.
    /// </summary>
    public class IgniteConfigurationSection : ConfigurationSection
    {
        /// <summary>
        /// Gets or sets the ignite configuration.
        /// </summary>
        /// <value>
        /// The ignite configuration.
        /// </value>
        public IgniteConfiguration IgniteConfiguration { get; private set; }

        /// <summary>
        /// Reads XML from the configuration file.
        /// </summary>
        /// <param name="reader">The reader object, which reads from the configuration file.</param>
        protected override void DeserializeSection(XmlReader reader)
        {
            IgniteArgumentCheck.NotNull(reader, "reader");

            IgniteConfiguration = IgniteConfigurationXmlSerializer.Deserialize<IgniteConfiguration>(reader);
        }

        /// <summary>
        /// Creates an XML string containing an unmerged view of the <see cref="ConfigurationSection" /> 
        /// object as a single section to write to a file.
        /// </summary>
        /// <param name="parentElement">The <see cref="ConfigurationElement" /> 
        /// instance to use as the parent when performing the un-merge.</param>
        /// <param name="name">The name of the section to create.</param>
        /// <param name="saveMode">The <see cref="ConfigurationSaveMode" /> instance 
        /// to use when writing to a string.</param>
        /// <returns>
        /// An XML string containing an unmerged view of the <see cref="ConfigurationSection" /> object.
        /// </returns>
        protected override string SerializeSection(ConfigurationElement parentElement, string name, 
            ConfigurationSaveMode saveMode)
        {
            IgniteArgumentCheck.NotNull(parentElement, "parentElement");
            IgniteArgumentCheck.NotNullOrEmpty(name, "name");

            if (IgniteConfiguration == null)
                return string.Format("<{0} />", name);

            var sb = new StringBuilder();

            using (var xmlWriter = XmlWriter.Create(sb))
            {
                IgniteConfigurationXmlSerializer.Serialize(IgniteConfiguration, xmlWriter, name);

                return sb.ToString();
            }
        }
    }
}
