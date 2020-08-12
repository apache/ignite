package org.apache.ignite.snippets;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.spi.encryption.keystore.KeystoreEncryptionSpi;

public class TDE {

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

        //tag::masterKeyRotation[]
        // Gets the current master key name.
        String name = ignite.encryption().getMasterKeyName();

        // Starts master key change process.
        IgniteFuture<Void> future = ignite.encryption().changeMasterKey("newMasterKeyName");
        //end::masterKeyRotation[]

        ignite.close();
    }
}
