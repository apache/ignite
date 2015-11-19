package org.apache.ignite.cache.store.cassandra.utils.session;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;

/**
 * Provides information for loadCache operation of {@link org.apache.ignite.cache.store.cassandra.CassandraCacheStore}.
 */
public interface BatchLoaderAssistant {
    /** TODO IGNITE-1371: add comment */
    public String operationName();

    /** TODO IGNITE-1371: add comment */
    public Statement getStatement();

    /** TODO IGNITE-1371: add comment */
    public void process(Row row);
}
