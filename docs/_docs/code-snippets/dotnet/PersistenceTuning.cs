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
    public class PersistenceTuning
    {
        public static void AdjustingPageSize()
        {
            // tag::page-size[]
            var cfg = new IgniteConfiguration
            {
                DataStorageConfiguration = new DataStorageConfiguration
                {
                    // Changing the page size to 4 KB.
                    PageSize = 4096
                }
            };
            // end::page-size[]
        }

        public static void KeepWalsSeparately()
        {
            // tag::separate-wal[]
            var cfg = new IgniteConfiguration
            {
                DataStorageConfiguration = new DataStorageConfiguration
                {
                    // Sets a path to the root directory where data and indexes are to be persisted.
                    // It's assumed the directory is on a separated SSD.
                    StoragePath = "/ssd/storage",
                    
                    // Sets a path to the directory where WAL is stored.
                    // It's assumed the directory is on a separated HDD.
                    WalPath = "/wal",
                    
                    // Sets a path to the directory where WAL archive is stored.
                    // The directory is on the same HDD as the WAL.
                    WalArchivePath = "/wal/archive"
                }
            };
            // end::separate-wal[]
        }

        public static void Throttling()
        {
            // tag::throttling[]
            var cfg = new IgniteConfiguration
            {
                DataStorageConfiguration = new DataStorageConfiguration
                {
                    WriteThrottlingEnabled = true
                }
            };
            // end::throttling[]
        }

        public static void CheckpointBufferSize()
        {
            // tag::checkpointing-buffer-size[]
            var cfg = new IgniteConfiguration
            {
                DataStorageConfiguration = new DataStorageConfiguration
                {
                    WriteThrottlingEnabled = true,
                    DefaultDataRegionConfiguration = new DataRegionConfiguration
                    {
                        Name = DataStorageConfiguration.DefaultDataRegionName,
                        PersistenceEnabled = true,
                        
                        // Increasing the buffer size to 1 GB.
                        CheckpointPageBufferSize = 1024L * 1024 * 1024
                    }
                }
            };
            // end::checkpointing-buffer-size[]
        }
    }
}
