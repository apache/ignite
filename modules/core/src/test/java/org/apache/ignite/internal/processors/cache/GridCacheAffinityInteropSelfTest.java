package org.apache.ignite.internal.processors.cache;

import java.util.Collections;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Affinity interoperability with other features
 */
public class GridCacheAffinityInteropSelfTest extends GridCommonAbstractTest {

    /**
     * IGNITE-5505 @AffinityKeyMapped annotation is ignored if class names are configured on BinaryConfiguration
     */
    public void testFieldAffinityMapperWithCustomBinaryConfiguration() throws Exception {
        BinaryConfiguration binaryCfg = new BinaryConfiguration();
        binaryCfg.setClassNames(Collections.singletonList(PositionKey.class.getName()));

        IgniteConfiguration igniteCfg = getConfiguration();
        igniteCfg.setBinaryConfiguration(binaryCfg);

        try (Ignite ignite = Ignition.start(igniteCfg)) {
            final String CACHE = "positions";
            ignite.getOrCreateCache(CACHE);
            GridCacheContext cacheCtx = ((IgniteEx)ignite).context().cache().internalCache(CACHE).context();
            PositionKey sberbank = new PositionKey(1, "US80585Y3080");
            Object affinityKey = cacheCtx.affinity().affinityKey(sberbank);
            assertEquals(sberbank.getIsin(), affinityKey);
        }
    }

    /** A key class with custom affinity mapping */
    private static class PositionKey {
        /** Position ID */
        private final int id;

        /** Instrument ISIN */
        @AffinityKeyMapped
        private final String isin;

        /** Constructor */
        PositionKey(int id, String isin) {
            this.id = id;
            this.isin = isin;
        }

        /** Get position ID */
        int getId() {
            return id;
        }

        /** Get instrument ISIN */
        String getIsin() {
            return isin;
        }
    }
}
