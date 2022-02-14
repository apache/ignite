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
package org.apache.ignite.snippets;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.DiskPageCompression;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.junit.jupiter.api.Test;

public class IgnitePersistence {

    @Test
    void disablingWal() {

        //tag::wal[]
        IgniteConfiguration cfg = new IgniteConfiguration();
        DataStorageConfiguration storageCfg = new DataStorageConfiguration();
        storageCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(true);

        cfg.setDataStorageConfiguration(storageCfg);

        Ignite ignite = Ignition.start(cfg);

        ignite.cluster().state(ClusterState.ACTIVE);

        String cacheName = "myCache";

        ignite.getOrCreateCache(cacheName);

        ignite.cluster().disableWal(cacheName);

        //load data
        ignite.cluster().enableWal(cacheName);

        //end::wal[]
        ignite.close();
    }

    @Test
    public static void changeWalSegmentSize() {
        // tag::segment-size[] 
        IgniteConfiguration cfg = new IgniteConfiguration();
        DataStorageConfiguration storageCfg = new DataStorageConfiguration();
        storageCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(true);

        storageCfg.setWalSegmentSize(128 * 1024 * 1024);

        cfg.setDataStorageConfiguration(storageCfg);

        Ignite ignite = Ignition.start(cfg);
        // end::segment-size[]

        ignite.close();
    }

    @Test
    public static void cfgExample() {
        //tag::cfg[]
        IgniteConfiguration cfg = new IgniteConfiguration();

        //data storage configuration
        DataStorageConfiguration storageCfg = new DataStorageConfiguration();

        storageCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(true);

        //tag::storage-path[]
        storageCfg.setStoragePath("/opt/storage");
        //end::storage-path[]

        cfg.setDataStorageConfiguration(storageCfg);

        Ignite ignite = Ignition.start(cfg);
        //end::cfg[]
        ignite.close();
    }

    @Test
    void walRecordsCompression() {
        //tag::wal-records-compression[]
        IgniteConfiguration cfg = new IgniteConfiguration();

        DataStorageConfiguration dsCfg = new DataStorageConfiguration();
        dsCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(true);

        //WAL page compression parameters
        dsCfg.setWalPageCompression(DiskPageCompression.LZ4);
        dsCfg.setWalPageCompressionLevel(8);

        cfg.setDataStorageConfiguration(dsCfg);
        Ignite ignite = Ignition.start(cfg);
        //end::wal-records-compression[]

        ignite.close();
    }

}
