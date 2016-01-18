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
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Configuration;
    using System.Diagnostics;
    using System.Reflection;
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

                    SetProperty(target, name, val);
                }
            }

            if (!reader.MoveToElement())
                return;

            while (reader.Read())
            {
                if (reader.NodeType != XmlNodeType.Element)
                    continue;

                var name = reader.Name;

                var prop = GetPropertyOrThrow(name, reader.Value, type);

                // Either (valueType|string), or ICollection, or reftype

                var propType = prop.PropertyType;

                if (propType.IsValueType || propType == typeof (string))
                {
                    // Regular property on xmlElement form
                    SetProperty(target, name, reader.Value);
                }
                else if (propType.IsGenericType && propType.GetGenericTypeDefinition() == typeof(ICollection<>))
                {
                    // TODO: read collection
                }
                else
                {
                    // Nested object (complex property)
                    var nestedVal = Activator.CreateInstance(propType);
                    prop.SetValue(target, nestedVal, null);
                    ReadElement(reader, nestedVal);
                }
            }
        }

        private static void SetProperty(object target, string propName, string propVal)
        {
            var type = target.GetType();
            var property = GetPropertyOrThrow(propName, propVal, type);

            var converter = GetConverter(property.PropertyType);

            // TODO: try-catch and wrap
            var convertedVal = converter.ConvertFromString(propVal);

            property.SetValue(target, convertedVal, null);
        }

        private static PropertyInfo GetPropertyOrThrow(string propName, string propVal, Type type)
        {
            var property = type.GetProperty(XmlNameToPropertyName(propName));

            if (property == null)
                throw new ConfigurationErrorsException(
                    string.Format(
                        "Invalid IgniteConfiguration attribute '{0}={1}', there is no such property on '{2}'",
                        propName, propVal, type));

            return property;
        }

        private static string XmlNameToPropertyName(string name)
        {
            Debug.Assert(name.Length > 0);

            return char.ToUpper(name[0]) + name.Substring(1);
        }

        private static TypeConverter GetConverter(Type type)
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
