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

namespace Apache.Ignite.Core.Tests.ApiParity
{
    using Apache.Ignite.Core.Cache.Configuration;
    using NUnit.Framework;

    /// <summary>
    /// Tests that .NET <see cref="QueryEntity"/> has all properties from Java configuration APIs.
    /// </summary>
    [Ignore(ParityTest.IgnoreReason)]
    public class QueryEntityConfigurationParityTest
    {
        /** Properties that are not needed on .NET side. */
        private static readonly string[] UnneededProperties =
        {
            "findKeyType",
            "findValueType",
            "KeyFields",
            "NotNullFields",
            "DefaultFieldValues",
            "FieldsPrecision",
            "FieldsScale"
        };

        /// <summary>
        /// Tests the ignite configuration parity.
        /// </summary>
        [Test]
        public void TestQueryEntityConfiguration()
        {
            ParityTest.CheckConfigurationParity(
                @"modules\core\src\main\java\org\apache\ignite\cache\QueryEntity.java", 
                typeof(QueryEntity),
                UnneededProperties);
        }
    }
}
