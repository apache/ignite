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
    using System.Collections.Generic;
    using Apache.Ignite.Core.Configuration;
    using NUnit.Framework;

    /// <summary>
    /// Tests queries with in-code configuration.
    /// </summary>
    public class CacheQueriesCodeConfigurationTest
    {
        /// <summary>
        /// Tests the SQL query.
        /// </summary>
        [Test]
        public void TestSqlQuery()
        {
            var cfg = new IgniteConfiguration
            {
                JvmOptions = TestUtils.TestJavaOptions(),
                JvmClasspath = TestUtils.CreateTestClasspath(),
                CacheConfiguration = new[]
                {
                    new CacheConfiguration
                    {
                        Name = "personCache",
                        QueryEntities = new[]
                        {
                            new QueryEntity
                            {
                                KeyTypeName = "Integer",
                                ValueTypeName = "QueryPerson",
                                FieldNames = new []
                                {
                                    new KeyValuePair<string, string>("Name", "String"), 
                                    new KeyValuePair<string, string>("Age", "Integer")
                                },
                                Indexes = new[]
                                {
                                    new QueryIndex {IndexType = QueryIndexType.FullText, FieldName = "Name"},
                                    new QueryIndex {IndexType = QueryIndexType.Sorted, FieldName = "Age"}
                                }
                            }
                        }
                    } 
                }
            };

            using (var ignite = Ignition.Start(cfg))
            {
                // TODO
            }
        }
    }
}
