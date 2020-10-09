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
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.encryption.keystore.KeystoreEncryptionSpi;

public class Snapshots {

    void configuration() {
        //tag::config[]
        IgniteConfiguration cfg = new IgniteConfiguration();

        File exSnpDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), "ex_snapshots", true);

        cfg.setSnapshotPath(exSnpDir.getAbsolutePath());
        //end::config[]

        Ignite ignite = Ignition.start(cfg);

        //tag::create[]
        CacheConfiguration<Long, String> ccfg = new CacheConfiguration<Long, String>("snapshot-cache");

        try (IgniteCache<Long, String> cache = ignite.getOrCreateCache(ccfg)) {
            cache.put(1, "Maxim");

            // Start snapshot operation.
            ignite.snapshot().createSnapshot("snapshot_02092020").get();
        }
        finally {
            ignite.destroyCache(ccfg);
        }
        //end::create[]
        
        ignite.close();
    }
}
