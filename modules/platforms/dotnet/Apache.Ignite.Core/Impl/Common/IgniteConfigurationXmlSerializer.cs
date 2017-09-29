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
    using Apache.Ignite.Core.Events;
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
            if (property != null)
            {
                if (!property.CanWrite && !IsKeyValuePair(property.DeclaringType))
                    return;

                if (IsObsolete(property))
                    return;
            }

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

            var realType = obj.GetType();

            // Specify type when it differs from declared type.
            if (valueType != realType)
            {
                writer.WriteAttributeString(TypNameAttribute, TypeStringConverter.Convert(obj.GetType()));
            }

            if (IsBasicType(obj.GetType()))
            {
                WriteBasicProperty(obj, writer, realType, null);
                return;
            }

            // Write attributes
            foreach (var prop in props.Where(p => IsBasicType(p.PropertyType) && !IsObsolete(p)))
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
                if (reader.Name == TypNameAttribute || reader.Name == XmlnsAttribute)
                    continue;

                var prop = GetPropertyOrThrow(reader.Name, reader.Value, target.GetType());

                var value = ConvertBasicValue(reader.Value, prop, prop.PropertyType);

                prop.SetValue(target, value, null);
            }

            // Read content
            reader.MoveToElement();

            while (reader.Read())
            {
                if (reader.NodeType != XmlNodeType.Element)
                    continue;

                var prop = GetPropertyOrThrow(reader.Name, reader.Value, targetType);

                var value = ReadPropertyValue(reader, resolver, prop, targetType);

                prop.SetValue(target, value, null);
            }
        }

        /// <summary>
        /// Reads the property value.
        /// </summary>
        private static object ReadPropertyValue(XmlReader reader, TypeResolver resolver, 
            PropertyInfo prop, Type targetType)
        {
            var propType = prop.PropertyType;

            if (propType == typeof(object))
            {
                propType = ResolvePropertyType(reader, propType, prop.Name, targetType, resolver);
            }

            if (IsBasicType(propType))
            {
                // Regular property in xmlElement form.
                return ConvertBasicValue(reader.ReadString(), prop, propType);
            }

            if (propType.IsGenericType && propType.GetGenericTypeDefinition() == typeof(ICollection<>))
            {
                // Collection.
                return ReadCollectionProperty(reader, prop, targetType, resolver);
            }

            if (propType.IsGenericType && propType.GetGenericTypeDefinition() == typeof(IDictionary<,>))
            {
                // Dictionary.
                return ReadDictionaryProperty(reader, prop, resolver);
            }

            // Nested object (complex property).
            return ReadComplexProperty(reader, propType, prop.Name, targetType, resolver);
        }

        /// <summary>
        /// Reads the complex property (nested object).
        /// </summary>
        private static object ReadComplexProperty(XmlReader reader, Type propType, string propName, Type targetType, 
            TypeResolver resolver)
        {
            propType = ResolvePropertyType(reader, propType, propName, targetType, resolver);

            var nestedVal = Activator.CreateInstance(propType);

            using (var subReader = reader.ReadSubtree())
            {
                subReader.Read();  // read first element

                ReadElement(subReader, nestedVal, resolver);
            }

            return nestedVal;
        }

        /// <summary>
        /// Resolves the type of the property.
        /// </summary>
        private static Type ResolvePropertyType(XmlReader reader, Type propType, string propName, Type targetType,
            TypeResolver resolver)
        {
            var typeName = reader.GetAttribute(TypNameAttribute);

            if (!propType.IsAbstract && typeName == null)
                return propType;

            var res = typeName == null
                ? null
                : resolver.ResolveType(typeName) ??
                  GetConcreteDerivedTypes(propType).FirstOrDefault(x => x.Name == typeName);

            if (res != null)
                return res;

            var message = string.Format("'type' attribute is required for '{0}.{1}' property", targetType.Name,
                propName);

            var derivedTypes = GetConcreteDerivedTypes(propType);


            if (typeName != null)
            {
                message += ", specified type cannot be resolved: " + typeName;
            }
            else if (derivedTypes.Any())
            {
                message += ", possible values are: " + string.Join(", ", derivedTypes.Select(x => x.Name));
            }

            throw new ConfigurationErrorsException(message);
        }

        /// <summary>
        /// Reads the collection.
        /// </summary>
        private static IList ReadCollectionProperty(XmlReader reader, PropertyInfo prop, Type targetType, 
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
                        : ReadComplexProperty(subReader, elementType, prop.Name, targetType, resolver));
                }
            }

            return list;
        }
        
        /// <summary>
        /// Reads the dictionary.
        /// </summary>
        private static IDictionary ReadDictionaryProperty(XmlReader reader, PropertyInfo prop, TypeResolver resolver)
        {
            var keyValTypes = prop.PropertyType.GetGenericArguments();

            var dictType = typeof (Dictionary<,>).MakeGenericType(keyValTypes);

            var pairType = typeof(Pair<,>).MakeGenericType(keyValTypes);

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

                    var pair = (IPair) Activator.CreateInstance(pairType);

                    var pairReader = subReader.ReadSubtree();

                    pairReader.Read();

                    ReadElement(pairReader, pair, resolver);

                    dict[pair.Key] = pair.Value;
                }
            }

            return dict;
        }

        /// <summary>
        /// Reads the basic value.
        /// </summary>
        private static object ConvertBasicValue(string propVal, PropertyInfo property, Type propertyType)
        {
            var converter = GetConverter(property, propertyType);

            return converter.ConvertFromInvariantString(propVal);
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
        private static PropertyInfo GetPropertyOrThrow(string propName, object propVal, Type type)
        {
            var property = type.GetProperty(XmlNameToPropertyName(propName));

            if (property == null)
            {
                throw new ConfigurationErrorsException(
                    string.Format(
                        "Invalid IgniteConfiguration attribute '{0}={1}', there is no such property on '{2}'",
                        propName, propVal, type));
            }

            if (!property.CanWrite)
            {
                throw new ConfigurationErrorsException(string.Format(
                        "Invalid IgniteConfiguration attribute '{0}={1}', property '{2}.{3}' is not writeable",
                        propName, propVal, type, property.Name));
            }

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

            if (IsKeyValuePair(propertyType))
                return false;

            return propertyType.IsValueType || propertyType == typeof (string) || propertyType == typeof (Type);
        }

        /// <summary>
        /// Determines whether specified type is KeyValuePair.
        /// </summary>
        private static bool IsKeyValuePair(Type propertyType)
        {
            Debug.Assert(propertyType != null);

            return propertyType.IsGenericType && propertyType.GetGenericTypeDefinition() == typeof (KeyValuePair<,>);
        }

        /// <summary>
        /// Gets converter for a property.
        /// </summary>
        private static TypeConverter GetConverter(PropertyInfo property, Type propertyType)
        {
            Debug.Assert(propertyType != null);

            if (propertyType.IsEnum)
                return new GenericEnumConverter(propertyType);

            if (propertyType == typeof (Type))
                return TypeStringConverter.Instance;

            if (propertyType == typeof(bool))
                return BooleanLowerCaseConverter.Instance;

            if (property != null &&
                property.DeclaringType == typeof (IgniteConfiguration) && property.Name == "IncludedEventTypes")
                return EventTypeConverter.Instance;

            if (property != null &&
                property.DeclaringType == typeof (LocalEventListener) && property.Name == "EventTypes")
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

            return obj.GetType().GetProperties()
                .Where(p => p.GetIndexParameters().Length == 0 &&  // Skip indexed properties.
                            !Equals(p.GetValue(obj, null), GetDefaultValue(p)));
        }

        /// <summary>
        /// Gets the default value for a property.
        /// </summary>
        private static object GetDefaultValue(PropertyInfo property)
        {
            var attr = property.GetCustomAttributes(true).OfType<DefaultValueAttribute>().FirstOrDefault();

            if (attr != null)
            {
                return attr.Value;
            }

            var declType = property.DeclaringType;
            if (declType != null && !declType.IsAbstract && declType.GetConstructor(new Type[0]) != null)
            {
                return property.GetValue(Activator.CreateInstance(declType), null);
            }

            var propertyType = property.PropertyType;

            if (propertyType.IsValueType)
            {
                return Activator.CreateInstance(propertyType);
            }

            return null;
        }

        /// <summary>
        /// Determines whether the specified property is obsolete.
        /// </summary>
        private static bool IsObsolete(PropertyInfo property)
        {
            Debug.Assert(property != null);

            return property.GetCustomAttributes(typeof(ObsoleteAttribute), true).Any();
        }

        /// <summary>
        /// Non-generic Pair accessor.
        /// </summary>
        private interface IPair
        {
            /// <summary>
            /// Gets the key.
            /// </summary>
            object Key { get; }

            /// <summary>
            /// Gets the value.
            /// </summary>
            object Value { get; }
        }

        /// <summary>
        /// Surrogate dictionary entry to overcome immutable KeyValuePair.
        /// </summary>
        private class Pair<TK, TV> : IPair
        {
            // ReSharper disable once UnusedAutoPropertyAccessor.Local
            // ReSharper disable once MemberCanBePrivate.Local
            public TK Key { get; set; }

            // ReSharper disable once UnusedAutoPropertyAccessor.Local
            // ReSharper disable once MemberCanBePrivate.Local
            public TV Value { get; set; }

            /** <inheritdoc /> */
            object IPair.Key
            {
                get { return Key; }
            }

            /** <inheritdoc /> */
            object IPair.Value
            {
                get { return Value; }
            }
        }
    }
}
