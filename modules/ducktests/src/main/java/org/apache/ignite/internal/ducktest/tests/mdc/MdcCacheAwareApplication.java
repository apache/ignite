package org.apache.ignite.internal.ducktest.tests.mdc;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.ducktest.tests.dto.IndexedDataRecord;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;
import org.apache.ignite.topology.MdcTopologyValidator;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Base class for MDC test applications.
 * Encapsulates the cache configuration (with {@link MdcTopologyValidator}) so that
 * generator and checkers always operate on an identically configured cache.
 */
public abstract class MdcCacheAwareApplication extends IgniteAwareApplication {
    /** JVM option name holding the data center identifier. */
    public static final String DC_ID_PROP = "IGNITE_DATA_CENTER_ID";

    /** */
    protected static String DFLT_CACHE_NAME = "default";

    /** */
    protected static int DFLT_BACKUPS = 0;

    /**
     * @param jNode Parameters.
     * @return Cache configured with the MDC topology validator.
     */
    protected IgniteCache<Integer, IndexedDataRecord> mdcCache(JsonNode jNode) {
        String cacheName = jNode.path("cacheName").asText(DFLT_CACHE_NAME);
        int backups = jNode.path("backups").asInt(DFLT_BACKUPS);

        String mainDc = jNode.path("mainDc").asText();

        MdcTopologyValidator topValidator = new MdcTopologyValidator();

        topValidator.setMainDatacenter(mainDc);

        CacheConfiguration<Integer, IndexedDataRecord> cacheCfg = new CacheConfiguration<Integer, IndexedDataRecord>()
            .setName(cacheName)
            .setTopologyValidator(topValidator)
            .setCacheMode(PARTITIONED)
            .setBackups(backups);

        return ignite.getOrCreateCache(cacheCfg);
    }

    /** */
    protected IgniteCache<Integer, IndexedDataRecord> mdcCache(String cacheName) {
        return ignite.cache(cacheName);
    }

    /**
     * @return Data center id this client belongs to (passed via {@code -DIGNITE_DATA_CENTER_ID=...}).
     */
    protected static String dcId() {
        return System.getProperty(DC_ID_PROP, "<undefined>");
    }
}
