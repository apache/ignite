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

namespace Apache.Ignite.Core.Impl.Events
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Diagnostics.CodeAnalysis;
    using System.Globalization;
    using System.Linq;
    using Apache.Ignite.Core.Events;

    /// <summary>
    /// Converts string to <see cref="EventType"/> member value.
    /// </summary>
    internal class EventTypeConverter : TypeConverter
    {
        /// <summary>
        /// Default instance.
        /// </summary>
        public static readonly EventTypeConverter Instance = new EventTypeConverter();

        /// <summary>
        /// The event type map.
        /// </summary>
        private static readonly Dictionary<int, string> EvtIdToNameMap =
            typeof (EventType).GetFields()
                .Where(p => p.FieldType == typeof (int))
                .ToDictionary(f => (int) f.GetValue(null), f => f.Name);

        /// <summary>
        /// The event type map.
        /// </summary>
        private static readonly Dictionary<string, int> EvtNameToIdMap =
            EvtIdToNameMap.ToDictionary(p => p.Value, p => p.Key, StringComparer.OrdinalIgnoreCase);

        /// <summary>
        /// Returns whether this converter can convert an object of the given type to the type of this converter, 
        /// using the specified context.
        /// </summary>
        /// <param name="context">An <see cref="ITypeDescriptorContext" /> that provides a format context.</param>
        /// <param name="sourceType">A <see cref="Type" /> that represents the type you want to convert from.</param>
        /// <returns>
        /// true if this converter can perform the conversion; otherwise, false.
        /// </returns>
        [ExcludeFromCodeCoverage]  // not called
        public override bool CanConvertFrom(ITypeDescriptorContext context, Type sourceType)
        {
            return sourceType == typeof(string);
        }

        /// <summary>
        /// Returns whether this converter can convert the object to the specified type, using the specified context.
        /// </summary>
        /// <param name="context">An <see cref="ITypeDescriptorContext" /> that provides a format context.</param>
        /// <param name="destinationType">
        /// A <see cref="Type" /> that represents the type you want to convert to.
        /// </param>
        /// <returns>
        /// true if this converter can perform the conversion; otherwise, false.
        /// </returns>
        [ExcludeFromCodeCoverage]  // not called
        public override bool CanConvertTo(ITypeDescriptorContext context, Type destinationType)
        {
            return destinationType == typeof(string);
        }

        /// <summary>
        /// Converts the given object to the type of this converter, 
        /// using the specified context and culture information.
        /// </summary>
        /// <param name="context">An <see cref="ITypeDescriptorContext" /> that provides a format context.</param>
        /// <param name="culture">The <see cref="CultureInfo" /> to use as the current culture.</param>
        /// <param name="value">The <see cref="Object" /> to convert.</param>
        /// <returns>
        /// An <see cref="object" /> that represents the converted value.
        /// </returns>
        public override object ConvertFrom(ITypeDescriptorContext context, CultureInfo culture, object value)
        {
            if (value == null)
                return null;

            var s = value.ToString();
            int intResult;

            if (int.TryParse(s, out intResult) || EvtNameToIdMap.TryGetValue(s, out intResult))
                return intResult;

            throw new InvalidOperationException(string.Format("Cannot convert value to {0}: {1}",
                typeof (EventType).Name, s));
        }

        /// <summary>
        /// Converts the given value object to the specified type, using the specified context and culture information.
        /// </summary>
        /// <param name="context">An <see cref="ITypeDescriptorContext" /> that provides a format context.</param>
        /// <param name="culture">
        /// A <see cref="CultureInfo" />. If null is passed, the current culture is assumed.
        /// </param>
        /// <param name="value">The <see cref="Object" /> to convert.</param>
        /// <param name="destinationType">
        /// The <see cref="Type" /> to convert the <paramref name="value" /> parameter to.
        /// </param>
        /// <returns>
        /// An <see cref="object" /> that represents the converted value.
        /// </returns>
        public override object ConvertTo(ITypeDescriptorContext context, CultureInfo culture, object value,
            Type destinationType)
        {
            if (!(value is int))
                return null;

            string eventName;

            if (EvtIdToNameMap.TryGetValue((int)value, out eventName))
                return eventName;

            return value.ToString();
        }
    }
}