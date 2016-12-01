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

namespace Apache.Ignite.Core.Tests.Cache.Query
{
    using NUnit.Framework;

    /// <summary>
    /// Tests Data Manipulation Language queries.
    /// </summary>
    public class CacheDmlQueriesTest
    {
        /// <summary>
        /// Tests the modification query.
        /// </summary>
        [Test]
        public void TestSqlDmlQuery()
        {
            // TODO: What if class has never been used before?
            // This does not matter because QueryEntity defines tables, not class itself.
            // However, we still should be able to get classes back?
        }

        /// <summary>
        /// Tests the modification query with a new cache and new query entity config.
        /// </summary>
        [Test]
        public void TestSqlDmlQueryDynamicCache()
        {
            // TODO: Create new cache, use binary-only mode?
        }
    }
}
