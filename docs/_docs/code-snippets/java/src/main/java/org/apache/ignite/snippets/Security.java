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
