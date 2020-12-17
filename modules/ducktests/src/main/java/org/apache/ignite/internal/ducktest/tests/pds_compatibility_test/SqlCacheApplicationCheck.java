package org.apache.ignite.internal.ducktest.tests.pds_compatibility_test;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;

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

        log.info("Check cache size");

        // Iterate over the result set.
        try (QueryCursor<List<?>> cursor = cache.query(sql)) {
            count = cursor.getAll().get(0).get(0).toString();
        } catch (Exception e) {
            e.printStackTrace();
        }

        log.info("SELECT COUNT(*) FROM ACCOUNT return: {}", count);

        assert count.equals(jsonNode.get("range").asText());

        sql = new SqlFieldsQuery(
                "explain SELECT * FROM Account WHERE postindex = 100");
        String explain = null;

        log.info("Check SQL Index");
        try (QueryCursor<List<?>> cursor = cache.query(sql)) {
            explain = cursor.getAll().get(0).get(0).toString();
        } catch (Exception e) {
            e.printStackTrace();
        }

        log.info("SQL Execution Plan: {}", explain);

        assert explain.contains("_IDX");

        log.info("Cache checked");

        markSyncExecutionComplete();
    }
}