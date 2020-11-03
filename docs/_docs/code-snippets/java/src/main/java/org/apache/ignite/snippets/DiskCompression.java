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
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.DiskPageCompression;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.junit.jupiter.api.Test;

public class DiskCompression {

    @Test
    void configuration() {
        //tag::configuration[]
        DataStorageConfiguration dsCfg = new DataStorageConfiguration();

        //set the page size to 2 types of the disk page size
        dsCfg.setPageSize(4096 * 2);

        //enable persistence for the default data region
        dsCfg.setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true));

        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setDataStorageConfiguration(dsCfg);

        CacheConfiguration cacheCfg = new CacheConfiguration("myCache");
        //enable disk page compression for this cache
        cacheCfg.setDiskPageCompression(DiskPageCompression.LZ4);
        //optionally set the compression level
        cacheCfg.setDiskPageCompressionLevel(10);

        cfg.setCacheConfiguration(cacheCfg);

        Ignite ignite = Ignition.start(cfg);
        //end::configuration[]
        
        ignite.close();
    }
}
