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

namespace Apache.Ignite.Core.Impl.Common
{
    using System;
    using System.ComponentModel;
    using System.Configuration;
    using System.Xml;

    /// <summary>
    /// Serializes <see cref="IgniteConfiguration"/> to XML.
    /// </summary>
    internal static class IgniteConfigurationXmlSerializer
    {
        public static IgniteConfiguration Deserialize(XmlReader reader)
        {
            var cfg = new IgniteConfiguration();

            ReadElement(reader, cfg);

            return cfg;
        }

        private static void ReadElement(XmlReader reader, object target)
        {
            // TODO: see http://referencesource.microsoft.com/#System.Configuration/System/Configuration/ConfigurationElement.cs
            var type = target.GetType();

            if (reader.AttributeCount > 0) // ???
            {
                while (reader.MoveToNextAttribute())
                {
                    var name = reader.Name;
                    var val = reader.Value;

                    // Set property
                    // TODO: lowercase first letter
                    type.GetProperty(name).SetValue(target, val, null);
                }
            }
        }

        private static object GetConverter(Type type)
        {
            // TODO: ICollection?
            if (type.IsEnum)
                return new GenericEnumConverter(type);

            var converter = TypeDescriptor.GetConverter(type);

            if (converter == null || !converter.CanConvertFrom(typeof(string)) ||
                !converter.CanConvertTo(typeof(string)))
                throw new ConfigurationErrorsException("No converter for type " + type);

            return converter;
        }
    }
}
