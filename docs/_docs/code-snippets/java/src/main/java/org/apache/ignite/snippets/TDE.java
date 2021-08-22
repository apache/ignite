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
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.spi.encryption.keystore.KeystoreEncryptionSpi;
import org.junit.jupiter.api.Test;

public class TDE {

    @Test
    void configuration() {
        //tag::config[]
        IgniteConfiguration cfg = new IgniteConfiguration();

        KeystoreEncryptionSpi encSpi = new KeystoreEncryptionSpi();

        encSpi.setKeyStorePath("/home/user/ignite-keystore.jks");
        encSpi.setKeyStorePassword("secret".toCharArray());

        cfg.setEncryptionSpi(encSpi);
        //end::config[]

        Ignite ignite = Ignition.start(cfg);

        //tag::cache[]
        CacheConfiguration<Long, String> ccfg = new CacheConfiguration<Long, String>("encrypted-cache");

        ccfg.setEncryptionEnabled(true);

        ignite.createCache(ccfg);

        //end::cache[]

        //tag::master-key-rotation[]
        // Gets the current master key name.
        String name = ignite.encryption().getMasterKeyName();

        // Starts master key change process.
        IgniteFuture<Void> future = ignite.encryption().changeMasterKey("newMasterKeyName");
        //end::master-key-rotation[]

        //tag::cache-group-key-rotation[]
        // Starts cache group encryption key change process.
        // This future will be completed when the new encryption key is set for writing on
        // all nodes in the cluster and re-encryption of existing cache data is initiated.
        IgniteFuture<Void> fut = ignite.encryption().changeCacheGroupKey(Collections.singleton("encrypted-cache"));
        //end::cache-group-key-rotation[]

        ignite.close();
    }
}
