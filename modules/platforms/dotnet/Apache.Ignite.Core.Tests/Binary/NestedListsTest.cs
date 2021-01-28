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

namespace Apache.Ignite.Core.Tests.Binary
{
    using System.Collections.Generic;
    using NUnit.Framework;

    /// <summary>
    /// TODO
    /// </summary>
    public class NestedListsTest
    {
        /// <summary>
        /// TODO
        /// </summary>
        [Test]
        public void Test()
        {
            // TODO: Why does not this fail offline in BinarySelfTest?
            var ignite = Ignition.Start(TestUtils.GetTestConfiguration());
            var cache = ignite.GetOrCreateCache<int, Entity[]>("c");

            cache.Put(1, new[]
            {
                new Entity {Inner = new List<object>()},
                new Entity {Inner = new List<object>()}
            });

            cache.Get(1);
        }

        private class Entity
        {
            public IList<object> Inner { get; set; }
        }
    }
}
