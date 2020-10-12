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
using Apache.Ignite.Core.Configuration;

namespace dotnet_helloworld
{
    class PersistenceIgnitePersistence
    {
        public static void DisablingWal()
        {
            // tag::disableWal[]
            var cacheName = "myCache";
            var ignite = Ignition.Start();
            ignite.GetCluster().DisableWal(cacheName);

            //load data

            ignite.GetCluster().EnableWal(cacheName);
            // end::disableWal[]
        }

        public static void Configuration()
        {
            // tag::cfg[]
            var cfg = new IgniteConfiguration
            {
                DataStorageConfiguration = new DataStorageConfiguration
                {
                  // tag::storage-path[]
                    StoragePath = "/ssd/storage",

                  // end::storage-path[]
                    DefaultDataRegionConfiguration = new DataRegionConfiguration
                    {
                        Name = "Default_Region",
                        PersistenceEnabled = true
                    }
                }
            };

            Ignition.Start(cfg);
            // end::cfg[]
        }

        public static void Swapping()
        {
            // tag::cfg-swap[]
            var cfg = new IgniteConfiguration
            {
                DataStorageConfiguration = new DataStorageConfiguration
                {
                    DataRegionConfigurations = new[]
                    {
                        new DataRegionConfiguration
                        {
                            Name = "5GB_Region",
                            InitialSize = 100L * 1024 * 1024,
                            MaxSize = 5L * 1024 * 1024 * 1024,
                            SwapPath = "/path/to/some/directory"
                        }
                    }
                }
            };
            // end::cfg-swap[]
        }
    }
}
