package org.apache.ignite.internal.ducktest.tests.pds_compatibility_test;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.Iterator;
import java.util.List;
import java.util.UUID;

public class SqlCacheApplicationCheck extends IgniteAwareApplication {
    /**
     * {@inheritDoc}
     */
    @Override
    protected void run(JsonNode jsonNode) {
        String count = null;
        log.info("Open cache...");

        IgniteCache<Long, Account> cache = ignite.cache(jsonNode.get("cacheName").asText());

        SqlFieldsQuery sql = new SqlFieldsQuery(
                "select count(*) from Account");
        cache.query(sql);

        // Iterate over the result set.
        try (QueryCursor<List<?>> cursor = cache.query(sql)) {
            Iterator<List<?>> iterator = cursor.iterator();
            if(iterator.hasNext()){
                count = iterator.next().get(0).toString();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        assert count.equals(jsonNode.get("range").asText());
        log.info("Cache checked");
        markSyncExecutionComplete();
    }
}