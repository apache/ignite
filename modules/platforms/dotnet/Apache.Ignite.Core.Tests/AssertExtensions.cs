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

namespace Apache.Ignite.Core.Tests
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Linq;
    using NUnit.Framework;

    /// <summary>
    /// NUnit assert extensions.
    /// </summary>
    internal static class AssertExtensions
    {
        /// <summary>
        /// Asserts equality with reflection recursively.
        /// </summary>
        public static void ReflectionEqual(object x, object y, string propertyPath = null,
            HashSet<string> ignoredProperties = null)
        {
            if (x == null && y == null)
            {
                return;
            }

            Assert.IsNotNull(x, propertyPath);
            Assert.IsNotNull(y, propertyPath);

            var type = x.GetType();

            if (type != typeof(string) && typeof(IEnumerable).IsAssignableFrom(type))
            {
                var xCol = ((IEnumerable)x).OfType<object>().ToList();
                var yCol = ((IEnumerable)y).OfType<object>().ToList();

                Assert.AreEqual(xCol.Count, yCol.Count, propertyPath);

                for (var i = 0; i < xCol.Count; i++)
                {
                    ReflectionEqual(xCol[i], yCol[i], propertyPath, ignoredProperties);
                }

                return;
            }

            Assert.AreEqual(type, y.GetType());

            propertyPath = propertyPath ?? type.Name;

            if (type.IsValueType || type == typeof(string) || type.IsSubclassOf(typeof(Type)))
            {
                Assert.AreEqual(x, y, propertyPath);
                return;
            }

            var props = type.GetProperties().Where(p => p.GetIndexParameters().Length == 0);

            foreach (var propInfo in props)
            {
                if (ignoredProperties != null && ignoredProperties.Contains(propInfo.Name))
                {
                    continue;
                }

                var propName = propertyPath + "." + propInfo.Name;

                var xVal = propInfo.GetValue(x, null);
                var yVal = propInfo.GetValue(y, null);

                ReflectionEqual(xVal, yVal, propName, ignoredProperties);
            }
        }
    }
}
