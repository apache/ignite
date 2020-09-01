package org.apache.ignite.snippets;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.DiskPageCompression;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.junit.jupiter.api.Test;

public class WAL {

    @Test
    void walRecordsCompression() {
        //tag::records-compression[]
        IgniteConfiguration cfg = new IgniteConfiguration();

        DataStorageConfiguration dsCfg = new DataStorageConfiguration();
        dsCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(true);

        //WAL page compression parameters
        dsCfg.setWalPageCompression(DiskPageCompression.LZ4);
        dsCfg.setWalPageCompressionLevel(8);

        cfg.setDataStorageConfiguration(dsCfg);
        Ignite ignite = Ignition.start(cfg);
        //end::records-compression[]
        
        ignite.close();
    }
}
