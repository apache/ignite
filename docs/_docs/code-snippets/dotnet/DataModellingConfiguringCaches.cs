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
    class DataModellingConfiguringCaches
    {
        public static void ConfigurationExample()
        {
            // tag::cfg[]
            var cfg = new IgniteConfiguration
            {
                CacheConfiguration = new[]
                {
                    new CacheConfiguration
                    {
                        Name = "myCache",
                        CacheMode = CacheMode.Partitioned,
                        Backups = 2,
                        RebalanceMode = CacheRebalanceMode.Sync,
                        WriteSynchronizationMode = CacheWriteSynchronizationMode.FullSync,
                        PartitionLossPolicy = PartitionLossPolicy.ReadOnlySafe
                    }
                }
            };
            Ignition.Start(cfg);
            // end::cfg[]
        }

        public static void Backups()
        {
            // tag::backups[]
            var cfg = new IgniteConfiguration
            {
                CacheConfiguration = new[]
                {
                    new CacheConfiguration
                    {
                        Name = "myCache",
                        CacheMode = CacheMode.Partitioned,
                        Backups = 1
                    }
                }
            };
            Ignition.Start(cfg);
            // end::backups[]
        }
        
        public static void AsyncBackups()
        {
            // tag::synchronization-mode[]
            var cfg = new IgniteConfiguration
            {
                CacheConfiguration = new[]
                {
                    new CacheConfiguration
                    {
                        Name = "myCache",
                        WriteSynchronizationMode = CacheWriteSynchronizationMode.FullSync,
                        Backups = 1
                    }
                }
            };
            Ignition.Start(cfg);
            // end::synchronization-mode[]
        }


        public static void CacheTemplates() 
        {
            // tag::template[]
            var ignite = Ignition.Start();

            var cfg = new CacheConfiguration
            {
                Name = "myCacheTemplate*",
                CacheMode = CacheMode.Partitioned,
                Backups = 2
            };

            ignite.AddCacheConfiguration(cfg);
            // end::template[]
        }

    }
}
