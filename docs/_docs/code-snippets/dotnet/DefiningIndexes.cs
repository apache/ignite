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
using Apache.Ignite.Core;
using Apache.Ignite.Core.Cache.Configuration;

namespace dotnet_helloworld
{
    //todo discuss about "Indexing Nested Objects"
    public class DefiningIndexes
    {
        // tag::idxAnnotationCfg[]
        class Person
        {
            // Indexed field. Will be visible to the SQL engine.
            [QuerySqlField(IsIndexed = true)] public long Id;

            //Queryable field. Will be visible to the SQL engine
            [QuerySqlField] public string Name;

            //Will NOT be visible to the SQL engine.
            public int Age;

            /** Indexed field sorted in descending order.
              * Will be visible to the SQL engine. */
            [QuerySqlField(IsIndexed = true, IsDescending = true)]
            public float Salary;
        }
        // end::idxAnnotationCfg[]

        //todo indexing nested objects - will be deprecated, discuss with Artem

        public static void RegisteringIndexedTypes()
        {
            // tag::register-indexed-types[]
            var ccfg = new CacheConfiguration
            {
                QueryEntities = new[]
                {
                    new QueryEntity(typeof(long), typeof(Person))
                }
            };
            // end::register-indexed-types[]
        }

        public class GroupIndexes
        {
            //todo functional is limited comparing to Java client, discuss
            // tag::groupIdx[]
            class Person
            {
                [QuerySqlField(IndexGroups = new[] {"age_salary_idx"})]
                public int Age;

                [QuerySqlField(IsIndexed = true, IndexGroups = new[] {"age_salary_idx"})]
                public double Salary;
            }
            // end::groupIdx[]
        }

        public static void QueryEntityDemo()
        {
            // tag::queryEntity[]
            var cacheCfg = new CacheConfiguration
            {
                Name = "myCache",
                QueryEntities = new[]
                {
                    new QueryEntity
                    {
                        KeyType = typeof(long),
                        KeyFieldName = "id",
                        ValueType = typeof(dotnet_helloworld.Person),
                        Fields = new[]
                        {
                            new QueryField
                            {
                                Name = "id",
                                FieldType = typeof(long)
                            },
                            new QueryField
                            {
                                Name = "name",
                                FieldType = typeof(string)
                            },
                            new QueryField
                            {
                                Name = "salary",
                                FieldType = typeof(long)
                            },
                        },
                        Indexes = new[]
                        {
                            new QueryIndex("name"),
                            new QueryIndex(false, QueryIndexType.Sorted, new[] {"id", "salary"})
                        }
                    }
                }
            };
            Ignition.Start(new IgniteConfiguration
            {
                CacheConfiguration = new[] {cacheCfg}
            });
            // end::queryEntity[]
        }

        private static void QueryEntityInlineSize()
        {
            // tag::query-entity-with-inline-size[]
            var qe = new QueryEntity
            {
                Indexes = new[]
                {
                    new QueryIndex
                    {
                        InlineSize = 13
                    }
                }
            };
            // end::query-entity-with-inline-size[]
        }

        private static void QueryEntityKeyFields()
        {
            // tag::custom-key[]
            var ccfg = new CacheConfiguration
            {
                Name = "personCache",
                QueryEntities = new[]
                {
                    new QueryEntity
                    {
                        KeyTypeName = "CustomKey",
                        ValueTypeName = "Person",
                        Fields = new[]
                        {
                            new QueryField
                            {
                                Name = "intKeyField",
                                FieldType = typeof(int),
                                IsKeyField = true
                            },
                            new QueryField
                            {
                                Name = "strKeyField",
                                FieldType = typeof(string),
                                IsKeyField = true
                            },
                            new QueryField
                            {
                                Name = "firstName",
                                FieldType = typeof(string)
                            },
                            new QueryField
                            {
                                Name = "lastName",
                                FieldType = typeof(string)
                            }
                        }
                    },
                }
            };
            // end::custom-key[]
        }

        private class InlineSize
        {
            // tag::annotation-with-inline-size[]
            [QuerySqlField(IsIndexed = true, IndexInlineSize = 13)]
            public string Country { get; set; }
            // end::annotation-with-inline-size[]
        }
    }
}
