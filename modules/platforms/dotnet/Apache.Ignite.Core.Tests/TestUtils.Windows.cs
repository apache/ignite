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
    using System.Linq;
    using Apache.Ignite.Core.Impl;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Common;
    using NUnit.Framework;

    /// <summary>
    /// Test utility methods - Windows-specific part (full framework).
    /// </summary>
    public static partial class TestUtils
    {
        /// <summary>
        ///
        /// </summary>
        /// <returns></returns>
        public static string CreateTestClasspath()
        {
            return Classpath.CreateClasspath(forceTestClasspath: true);
        }

        /// <summary>
        /// Asserts that the handle registry is empty.
        /// </summary>
        /// <param name="timeout">Timeout, in milliseconds.</param>
        /// <param name="grids">Grids to check.</param>
        public static void AssertHandleRegistryIsEmpty(int timeout, params IIgnite[] grids)
        {
            foreach (var g in grids)
                AssertHandleRegistryHasItems(g, 0, timeout);
        }

        /// <summary>
        /// Asserts that the handle registry has specified number of entries.
        /// </summary>
        /// <param name="timeout">Timeout, in milliseconds.</param>
        /// <param name="expectedCount">Expected item count.</param>
        /// <param name="grids">Grids to check.</param>
        public static void AssertHandleRegistryHasItems(int timeout, int expectedCount, params IIgnite[] grids)
        {
            foreach (var g in grids)
                AssertHandleRegistryHasItems(g, expectedCount, timeout);
        }

        /// <summary>
        /// Asserts that the handle registry has specified number of entries.
        /// </summary>
        /// <param name="grid">The grid to check.</param>
        /// <param name="expectedCount">Expected item count.</param>
        /// <param name="timeout">Timeout, in milliseconds.</param>
        public static void AssertHandleRegistryHasItems(IIgnite grid, int expectedCount, int timeout)
        {
            var handleRegistry = ((Ignite)grid).HandleRegistry;

            expectedCount++;  // Skip default lifecycle bean

            if (WaitForCondition(() => handleRegistry.Count == expectedCount, timeout))
                return;

            var items = handleRegistry.GetItems().Where(x => !(x.Value is LifecycleHandlerHolder)).ToList();

            if (items.Any())
                Assert.Fail("HandleRegistry is not empty in grid '{0}' (expected {1}, actual {2}):\n '{3}'",
                    grid.Name, expectedCount, handleRegistry.Count,
                    items.Select(x => x.ToString()).Aggregate((x, y) => x + "\n" + y));
        }

        /// <summary>
        /// Serializes and deserializes back an object.
        /// </summary>
        public static T SerializeDeserialize<T>(T obj)
        {
            var marsh = new Marshaller(null) { CompactFooter = false };

            return marsh.Unmarshal<T>(marsh.Marshal(obj));
        }

        /// <summary>
        /// Gets the name of the temporary directory.
        /// </summary>
        public static string GetTempDirectoryName()
        {
            return IgniteUtils.GetTempDirectoryName();
        }
    }
}
