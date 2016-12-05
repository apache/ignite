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
    using System.Collections;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Configuration;
    using System.Diagnostics;
    using System.Linq;
    using System.Reflection;
    using System.Xml;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Events;

    /// <summary>
    /// Serializes <see cref="IgniteConfiguration"/> to XML.
    /// </summary>
    internal static class IgniteConfigurationXmlSerializer
    {
        /** Attribute that specifies a type for abstract properties, such as IpFinder. */
        private const string TypNameAttribute = "type";

        /** Xmlns. */
        private const string XmlnsAttribute = "xmlns";

        /** Xmlns. */
        private const string KeyValPairElement = "pair";

        /** Schema. */
        private const string Schema = "http://ignite.apache.org/schema/dotnet/IgniteConfigurationSection";

        /// <summary>
        /// Deserializes <see cref="IgniteConfiguration"/> from specified <see cref="XmlReader"/>.
        /// </summary>
        /// <param name="reader">The reader.</param>
        /// <returns>Resulting <see cref="IgniteConfiguration"/>.</returns>
        public static IgniteConfiguration Deserialize(XmlReader reader)
        {
            IgniteArgumentCheck.NotNull(reader, "reader");

            var cfg = new IgniteConfiguration();

            if (reader.NodeType == XmlNodeType.Element || reader.Read())
                ReadElement(reader, cfg, new TypeResolver());

            return cfg;
        }

        /// <summary>
        /// Serializes specified <see cref="IgniteConfiguration" /> to <see cref="XmlWriter" />.
        /// </summary>
        /// <param name="configuration">The configuration.</param>
        /// <param name="writer">The writer.</param>
        /// <param name="rootElementName">Name of the root element.</param>
        public static void Serialize(IgniteConfiguration configuration, XmlWriter writer, string rootElementName)
        {
            IgniteArgumentCheck.NotNull(configuration, "configuration");
            IgniteArgumentCheck.NotNull(writer, "writer");
            IgniteArgumentCheck.NotNullOrEmpty(rootElementName, "rootElementName");

            WriteElement(configuration, writer, rootElementName, typeof(IgniteConfiguration));
        }

        /// <summary>
        /// Writes new element.
        /// </summary>
        private static void WriteElement(object obj, XmlWriter writer, string rootElementName, Type valueType, 
            PropertyInfo property = null)
        {
            if (valueType == typeof(IgniteConfiguration))
                writer.WriteStartElement(rootElementName, Schema);  // write xmlns for the root element
            else
                writer.WriteStartElement(rootElementName);

            if (IsBasicType(valueType))
                WriteBasicProperty(obj, writer, valueType, property);
            else if (valueType.IsGenericType && valueType.GetGenericTypeDefinition() == typeof (ICollection<>))
                WriteCollectionProperty(obj, writer, valueType, property);
            else if (valueType.IsGenericType && valueType.GetGenericTypeDefinition() == typeof (IDictionary<,>))
                WriteDictionaryProperty(obj, writer, valueType, property);
            else
                WriteComplexProperty(obj, writer, valueType);

            writer.WriteEndElement();
        }

        /// <summary>
        /// Writes the property of a basic type (primitives, strings, types).
        /// </summary>
        private static void WriteBasicProperty(object obj, XmlWriter writer, Type valueType, PropertyInfo property)
        {
            var converter = GetConverter(property, valueType);

            var stringValue = converter.ConvertToInvariantString(obj);

            writer.WriteString(stringValue ?? "");
        }

        /// <summary>
        /// Writes the collection property.
        /// </summary>
        private static void WriteCollectionProperty(object obj, XmlWriter writer, Type valueType, PropertyInfo property)
        {
            var elementType = valueType.GetGenericArguments().Single();

            var elementTypeName = PropertyNameToXmlName(elementType.Name);

            foreach (var element in (IEnumerable)obj)
                WriteElement(element, writer, elementTypeName, elementType, property);
        }

        /// <summary>
        /// Writes the dictionary property.
        /// </summary>
        private static void WriteDictionaryProperty(object obj, XmlWriter writer, Type valueType, PropertyInfo property)
        {
            var elementType = typeof (KeyValuePair<,>).MakeGenericType(valueType.GetGenericArguments());

            foreach (var element in (IEnumerable)obj)
                WriteElement(element, writer, KeyValPairElement, elementType, property);
        }

        /// <summary>
        /// Writes the complex property (nested object).
        /// </summary>
        private static void WriteComplexProperty(object obj, XmlWriter writer, Type valueType)
        {
            var props = GetNonDefaultProperties(obj).OrderBy(x => x.Name).ToList();

            // Specify type for interfaces and abstract classes
            if (valueType.IsAbstract)
                writer.WriteAttributeString(TypNameAttribute, TypeStringConverter.Convert(obj.GetType()));

            // Write attributes
            foreach (var prop in props.Where(p => IsBasicType(p.PropertyType)))
            {
                var converter = GetConverter(prop, prop.PropertyType);
                var stringValue = converter.ConvertToInvariantString(prop.GetValue(obj, null));
                writer.WriteAttributeString(PropertyNameToXmlName(prop.Name), stringValue ?? "");
            }

            // Write elements
            foreach (var prop in props.Where(p => !IsBasicType(p.PropertyType)))
                WriteElement(prop.GetValue(obj, null), writer, PropertyNameToXmlName(prop.Name),
                    prop.PropertyType, prop);
        }

        /// <summary>
        /// Reads the element.
        /// </summary>
        private static void ReadElement(XmlReader reader, object target, TypeResolver resolver)
        {
            var targetType = target.GetType();

            // Read attributes
            while (reader.MoveToNextAttribute())
            {
                var name = reader.Name;
                var val = reader.Value;

                SetProperty(target, name, val);
            }

            // Read content
            reader.MoveToElement();

            while (reader.Read())
            {
                if (reader.NodeType != XmlNodeType.Element)
                    continue;

                var name = reader.Name;
                var prop = GetPropertyOrThrow(name, reader.Value, targetType);
                var propType = prop.PropertyType;

                if (IsBasicType(propType))
                {
                    // Regular property in xmlElement form
                    SetProperty(target, name, reader.ReadString());
                }
                else if (propType.IsGenericType && propType.GetGenericTypeDefinition() == typeof (ICollection<>))
                {
                    // Collection
                    ReadCollectionProperty(reader, prop, target, resolver);
                }
                else if (propType.IsGenericType && propType.GetGenericTypeDefinition() == typeof (IDictionary<,>))
                {
                    // Dictionary
                    ReadDictionaryProperty(reader, prop, target);
                }
                else
                {
                    // Nested object (complex property)
                    prop.SetValue(target, ReadComplexProperty(reader, propType, prop.Name, targetType, resolver), null);
                }
            }
        }

        /// <summary>
        /// Reads the complex property (nested object).
        /// </summary>
        private static object ReadComplexProperty(XmlReader reader, Type propType, string propName, Type targetType, 
            TypeResolver resolver)
        {
            if (propType.IsAbstract)
            {
                var typeName = reader.GetAttribute(TypNameAttribute);

                var derivedTypes = GetConcreteDerivedTypes(propType);

                propType = typeName == null
                    ? null
                    : resolver.ResolveType(typeName) ?? derivedTypes.FirstOrDefault(x => x.Name == typeName);

                if (propType == null)
                {
                    var message = string.Format("'type' attribute is required for '{0}.{1}' property", targetType.Name,
                        propName);

                    if (typeName != null)
                    {
                        message += ", specified type cannot be resolved: " + typeName;
                    }
                    else if (derivedTypes.Any())
                        message += ", possible values are: " + string.Join(", ", derivedTypes.Select(x => x.Name));

                    throw new ConfigurationErrorsException(message);
                }
            }

            var nestedVal = Activator.CreateInstance(propType);

            using (var subReader = reader.ReadSubtree())
            {
                subReader.Read();  // read first element

                ReadElement(subReader, nestedVal, resolver);
            }

            return nestedVal;
        }

        /// <summary>
        /// Reads the collection.
        /// </summary>
        private static void ReadCollectionProperty(XmlReader reader, PropertyInfo prop, object target, 
            TypeResolver resolver)
        {
            var elementType = prop.PropertyType.GetGenericArguments().Single();

            var listType = typeof (List<>).MakeGenericType(elementType);

            var list = (IList) Activator.CreateInstance(listType);

            var converter = IsBasicType(elementType) ? GetConverter(prop, elementType) : null;

            using (var subReader = reader.ReadSubtree())
            {
                subReader.Read();  // skip list head
                while (subReader.Read())
                {
                    if (subReader.NodeType != XmlNodeType.Element)
                        continue;

                    if (subReader.Name != PropertyNameToXmlName(elementType.Name))
                        throw new ConfigurationErrorsException(
                            string.Format("Invalid list element in IgniteConfiguration: expected '{0}', but was '{1}'",
                                PropertyNameToXmlName(elementType.Name), subReader.Name));

                    list.Add(converter != null
                        ? converter.ConvertFromInvariantString(subReader.ReadString())
                        : ReadComplexProperty(subReader, elementType, prop.Name, target.GetType(), resolver));
                }
            }

            prop.SetValue(target, list, null);
        }
        
        /// <summary>
        /// Reads the dictionary.
        /// </summary>
        private static void ReadDictionaryProperty(XmlReader reader, PropertyInfo prop, object target)
        {
            var keyValTypes = prop.PropertyType.GetGenericArguments();

            var dictType = typeof (Dictionary<,>).MakeGenericType(keyValTypes);

            var dict = (IDictionary) Activator.CreateInstance(dictType);

            using (var subReader = reader.ReadSubtree())
            {
                subReader.Read();  // skip list head
                while (subReader.Read())
                {
                    if (subReader.NodeType != XmlNodeType.Element)
                        continue;

                    if (subReader.Name != PropertyNameToXmlName(KeyValPairElement))
                        throw new ConfigurationErrorsException(
                            string.Format("Invalid dictionary element in IgniteConfiguration: expected '{0}', " +
                                          "but was '{1}'", KeyValPairElement, subReader.Name));

                    var key = subReader.GetAttribute("key");

                    if (key == null)
                        throw new ConfigurationErrorsException(
                            "Invalid dictionary entry, key attribute is missing for property " + prop);

                    dict[key] = subReader.GetAttribute("value");
                }
            }

            prop.SetValue(target, dict, null);
        }

        /// <summary>
        /// Sets the property.
        /// </summary>
        private static void SetProperty(object target, string propName, string propVal)
        {
            if (propName == TypNameAttribute || propName == XmlnsAttribute)
                return;

            var type = target.GetType();
            var property = GetPropertyOrThrow(propName, propVal, type);

            var converter = GetConverter(property, property.PropertyType);

            var convertedVal = converter.ConvertFromInvariantString(propVal);

            property.SetValue(target, convertedVal, null);
        }

        /// <summary>
        /// Gets concrete derived types.
        /// </summary>
        private static List<Type> GetConcreteDerivedTypes(Type type)
        {
            return typeof(IIgnite).Assembly.GetTypes()
                .Where(t => t.IsClass && !t.IsAbstract && type.IsAssignableFrom(t)).ToList();
        }

        /// <summary>
        /// Gets specified property from a type or throws an exception.
        /// </summary>
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

        /// <summary>
        /// Converts an XML name to CLR name.
        /// </summary>
        private static string XmlNameToPropertyName(string name)
        {
            Debug.Assert(name.Length > 0);

            if (name == "int")
                return "Int32";  // allow aliases

            return char.ToUpperInvariant(name[0]) + name.Substring(1);
        }

        /// <summary>
        /// Converts a CLR name to XML name.
        /// </summary>
        private static string PropertyNameToXmlName(string name)
        {
            Debug.Assert(name.Length > 0);

            if (name == "Int32")
                return "int";  // allow aliases

            return char.ToLowerInvariant(name[0]) + name.Substring(1);
        }

        /// <summary>
        /// Determines whether specified type is a basic built-in type.
        /// </summary>
        private static bool IsBasicType(Type propertyType)
        {
            Debug.Assert(propertyType != null);

            if (propertyType.IsGenericType && propertyType.GetGenericTypeDefinition() == typeof (KeyValuePair<,>))
                return false;

            return propertyType.IsValueType || propertyType == typeof (string) || propertyType == typeof (Type) ||
                   propertyType == typeof (object);
        }

        /// <summary>
        /// Gets converter for a property.
        /// </summary>
        private static TypeConverter GetConverter(PropertyInfo property, Type propertyType)
        {
            Debug.Assert(property != null);
            Debug.Assert(propertyType != null);

            if (propertyType.IsEnum)
                return new GenericEnumConverter(propertyType);

            if (propertyType == typeof (Type))
                return TypeStringConverter.Instance;

            if (propertyType == typeof(bool))
                return BooleanLowerCaseConverter.Instance;

            if (property.DeclaringType == typeof (IgniteConfiguration) && property.Name == "IncludedEventTypes")
                return EventTypeConverter.Instance;

            if (propertyType == typeof (object))
                return ObjectStringConverter.Instance;

            var converter = TypeDescriptor.GetConverter(propertyType);

            if (converter == null || !converter.CanConvertFrom(typeof(string)) ||
                !converter.CanConvertTo(typeof(string)))
                throw new ConfigurationErrorsException("No converter for type " + propertyType);

            return converter;
        }

        /// <summary>
        /// Gets properties with non-default value.
        /// </summary>
        private static IEnumerable<PropertyInfo> GetNonDefaultProperties(object obj)
        {
            Debug.Assert(obj != null);

            return obj.GetType().GetProperties().Where(p => !Equals(p.GetValue(obj, null), GetDefaultValue(p)));
        }

        /// <summary>
        /// Gets the default value for a property.
        /// </summary>
        private static object GetDefaultValue(PropertyInfo property)
        {
            var attr = property.GetCustomAttributes(true).OfType<DefaultValueAttribute>().FirstOrDefault();

            if (attr != null)
                return attr.Value;

            var propertyType = property.PropertyType;

            if (propertyType.IsValueType)
                return Activator.CreateInstance(propertyType);

            return null;
        }
    }
}
