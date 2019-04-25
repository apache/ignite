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

namespace Apache.Ignite.Core.Impl.Common
{
    using System;
    using System.ComponentModel;
    using System.Diagnostics.CodeAnalysis;
    using System.Globalization;

    /// <summary>
    /// Bollean converter that returns lower-case strings, for XML serialization.
    /// </summary>
    internal class BooleanLowerCaseConverter : BooleanConverter
    {
        /// <summary>
        /// Default instance.
        /// </summary>
        public static readonly BooleanLowerCaseConverter Instance = new BooleanLowerCaseConverter();

        /// <summary>
        /// Converts the given value object to the specified type, using the specified context and culture information.
        /// </summary>
        /// <param name="context">An <see cref="ITypeDescriptorContext" /> that provides a format context.</param>
        /// <param name="culture">
        /// A <see cref="CultureInfo" />. If null is passed, the current culture is assumed.
        /// </param>
        /// <param name="value">The <see cref="object" /> to convert.</param>
        /// <param name="destinationType">
        /// The <see cref="Type" /> to convert the <paramref name="value" /> parameter to.
        /// </param>
        /// <returns>
        /// An <see cref="object" /> that represents the converted value.
        /// </returns>
        [SuppressMessage("Microsoft.Globalization", "CA1308:NormalizeStringsToUppercase")]
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "2")]
        public override object ConvertTo(ITypeDescriptorContext context, CultureInfo culture, object value, 
            Type destinationType)
        {
            if (destinationType == typeof (string))
                return value.ToString().ToLowerInvariant();

            return base.ConvertTo(context, culture, value, destinationType);
        }
    }
}
