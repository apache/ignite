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
    class MemoryArchitecture
    {
        public static void MemoryConfiguration()
        {
            // tag::mem[]
            var cfg = new IgniteConfiguration
            {
                DataStorageConfiguration = new DataStorageConfiguration
                {
                    DefaultDataRegionConfiguration = new DataRegionConfiguration
                    {
                        Name = "Default_Region",
                        InitialSize = 100 * 1024 * 1024
                    },
                    DataRegionConfigurations = new[]
                    {
                        new DataRegionConfiguration
                        {
                            Name = "40MB_Region_Eviction",
                            InitialSize = 20 * 1024 * 1024,
                            MaxSize = 40 * 1024 * 1024,
                            PageEvictionMode = DataPageEvictionMode.Random2Lru
                        },
                        new DataRegionConfiguration
                        {
                            Name = "30MB_Region_Swapping",
                            InitialSize = 15 * 1024 * 1024,
                            MaxSize = 30 * 1024 * 1024,
                            SwapPath = "/path/to/swap/file"
                        }
                    }
                }
            };
            Ignition.Start(cfg);
            // end::mem[]
        }

        public static void DefaultDataReqion()
        {
             // tag::DefaultDataReqion[]
             var cfg = new IgniteConfiguration
             {
                 DataStorageConfiguration = new DataStorageConfiguration
                 {
                     DefaultDataRegionConfiguration = new DataRegionConfiguration
                     {
                         Name = "Default_Region",
                         InitialSize = 100 * 1024 * 1024
                     }
                 }
             };

             // Start the node.
             var ignite = Ignition.Start(cfg);
             // end::DefaultDataReqion[]
        }
    }
}
