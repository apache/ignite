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
using Apache.Ignite.Core.Binary;
using Apache.Ignite.Core.Cache;
using Apache.Ignite.Core.Discovery.Tcp;
using Apache.Ignite.Core.Discovery.Tcp.Static;

namespace dotnet_helloworld
{
    public class WorkingWithBinaryObjects
    {
        class Book
        {
            public string Title { get; set; }
        }

        class MyEntryProcessor : ICacheEntryProcessor<int, IBinaryObject, object, object>
        {
            public object Process(IMutableCacheEntry<int, IBinaryObject> entry, object arg)
            {
                // Create a builder from the old value
                var bldr = entry.Value.ToBuilder();

                //Update the field in the builder
                bldr.SetField("Name", "Ignite");

                // Set new value to the entry
                entry.Value = bldr.Build();

                return null;
            }
        }

        public static void EntryProcessorForBinaryObjectDemo()
        {
            var ignite = Ignition.Start(new IgniteConfiguration
            {
                DiscoverySpi = new TcpDiscoverySpi
                {
                    LocalPort = 48500,
                    LocalPortRange = 20,
                    IpFinder = new TcpDiscoveryStaticIpFinder
                    {
                        Endpoints = new[]
                        {
                            "127.0.0.1:48500..48520"
                        }
                    }
                }
            });


            var cache = ignite.CreateCache<int, object>("cacheName");
            var key = 101;
            cache.Put(key, new Book {Title = "book_name"});

            cache
                .WithKeepBinary<int, IBinaryObject>()
                .Invoke(key, new MyEntryProcessor(), null);
            // Not supported yet: https://issues.apache.org/jira/browse/IGNITE-3825
        }
        // tag::entryProcessor[]
        // Not supported in C# for now
        // end::entryProcessor[]

        public class ExampleGlobalNameMapper : IBinaryNameMapper
        {
            public string GetTypeName(string name)
            {
                throw new System.NotImplementedException();
            }

            public string GetFieldName(string name)
            {
                throw new System.NotImplementedException();
            }
        }

        public class ExampleGlobalIdMapper : IBinaryIdMapper
        {
            public int GetTypeId(string typeName)
            {
                throw new System.NotImplementedException();
            }

            public int GetFieldId(int typeId, string fieldName)
            {
                throw new System.NotImplementedException();
            }
        }

        public class ExampleSerializer : IBinarySerializer
        {
            public void WriteBinary(object obj, IBinaryWriter writer)
            {
                throw new System.NotImplementedException();
            }

            public void ReadBinary(object obj, IBinaryReader reader)
            {
                throw new System.NotImplementedException();
            }
        }

        public static void ConfiguringBinaryObjects()
        {
            // tag::binaryCfg[]
            var cfg = new IgniteConfiguration
            {
                BinaryConfiguration = new BinaryConfiguration
                {
                    NameMapper = new ExampleGlobalNameMapper(),
                    IdMapper = new ExampleGlobalIdMapper(),
                    TypeConfigurations = new[]
                    {
                        new BinaryTypeConfiguration
                        {
                            TypeName = "org.apache.ignite.examples.*",
                            Serializer = new ExampleSerializer()
                        }
                    }
                }
            };
            // end::binaryCfg[]
        }
    }
}
