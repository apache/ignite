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

namespace Apache.Ignite.Core.Impl.Binary
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Reflection;

    /// <summary>
    /// Reflection utils.
    /// </summary>
    internal static class ReflectionUtils
    {
        /// <summary>
        /// Gets all fields, including base classes.
        /// </summary>
        public static IEnumerable<FieldInfo> GetAllFields(Type type)
        {
            const BindingFlags flags = BindingFlags.Instance | BindingFlags.Public |
                                       BindingFlags.NonPublic | BindingFlags.DeclaredOnly;

            var curType = type;

            while (curType != null)
            {
                foreach (var field in curType.GetFields(flags))
                {
                    yield return field;
                }

                curType = curType.BaseType;
            }
        }

        /// <summary>
        /// Gets all fields and properties, including base classes.
        /// </summary>
        /// <param name="type">The type.</param>
        public static IEnumerable<KeyValuePair<MemberInfo, Type>> GetFieldsAndProperties(Type type)
        {
            Debug.Assert(type != null);

            if (type.IsPrimitive)
                yield break;

            const BindingFlags bindingFlags = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance |
                                              BindingFlags.DeclaredOnly;

            while (type != typeof(object) && type != null)
            {
                foreach (var fieldInfo in type.GetFields(bindingFlags))
                    yield return new KeyValuePair<MemberInfo, Type>(fieldInfo, fieldInfo.FieldType);

                foreach (var propertyInfo in type.GetProperties(bindingFlags))
                    yield return new KeyValuePair<MemberInfo, Type>(propertyInfo, propertyInfo.PropertyType);

                type = type.BaseType;
            }
        }
    }
}
