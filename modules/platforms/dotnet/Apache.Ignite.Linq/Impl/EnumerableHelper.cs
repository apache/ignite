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

namespace Apache.Ignite.Linq.Impl
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;

    /// <summary>
    /// Contains static methods to work with IEnumerable
    /// </summary>
    internal static class EnumerableHelper
    {
        /// <summary>
        /// Gets item type of enumerable
        /// </summary>
        public static Type GetIEnumerableItemType(Type type)
        {
            Debug.Assert(type != null);

            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IEnumerable<>))
            {
                return type.GetGenericArguments()[0];
            }

            var implementedIEnumerableType = type.GetInterfaces()
                .FirstOrDefault(t => t.IsGenericType && t.GetGenericTypeDefinition() == typeof(IEnumerable<>));

            if (implementedIEnumerableType != null)
            {
                return implementedIEnumerableType.GetGenericArguments()[0];
            }

            if (type == typeof(IEnumerable))
            {
                return typeof(object);
            }

            throw new NotSupportedException("Type is not IEnumerable: " + type.FullName);
        }
    }
}