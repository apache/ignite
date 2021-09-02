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
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.ssl.SslContextFactory;
import org.junit.jupiter.api.Test;

public class Security {

    @Test
    void ssl() {
        // tag::ssl-context-factory[]
        IgniteConfiguration igniteCfg = new IgniteConfiguration();

        SslContextFactory factory = new SslContextFactory();

        factory.setKeyStoreFilePath("keystore/node.jks");
        factory.setKeyStorePassword("123456".toCharArray());
        factory.setTrustStoreFilePath("keystore/trust.jks");
        factory.setTrustStorePassword("123456".toCharArray());
        factory.setProtocol("TLSv1.3");

        igniteCfg.setSslContextFactory(factory);
        // end::ssl-context-factory[]

        Ignition.start(igniteCfg).close();
    }

    @Test
    void disableCertificateValidation() {
        // tag::disable-validation[]
        IgniteConfiguration igniteCfg = new IgniteConfiguration();

        SslContextFactory factory = new SslContextFactory();

        factory.setKeyStoreFilePath("keystore/node.jks");
        factory.setKeyStorePassword("123456".toCharArray());
        factory.setTrustManagers(SslContextFactory.getDisabledTrustManager());

        igniteCfg.setSslContextFactory(factory);
        // end::disable-validation[]

        Ignition.start(igniteCfg).close();
    }

    @Test
    void igniteAuthentication() {

        // tag::ignite-authentication[]
        IgniteConfiguration cfg = new IgniteConfiguration();

        // Ignite persistence configuration.
        DataStorageConfiguration storageCfg = new DataStorageConfiguration();

        // Enabling the persistence.
        storageCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(true);

        // Applying settings.
        cfg.setDataStorageConfiguration(storageCfg);

        // Enable authentication
        cfg.setAuthenticationEnabled(true);

        Ignite ignite = Ignition.start(cfg);
        // end::ignite-authentication[]

        ignite.close();
    }

    
    

    
    
    
}
