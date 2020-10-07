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
