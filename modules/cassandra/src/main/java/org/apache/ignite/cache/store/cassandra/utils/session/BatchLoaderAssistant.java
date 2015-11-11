package org.apache.ignite.cache.store.cassandra.utils.session;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;

/**
 * Provides information for loadCache operation of ${@link org.apache.ignite.cache.store.cassandra.CassandraCacheStore}
 */
public interface BatchLoaderAssistant {
    public String operationName();

    public Statement getStatement();

    public void process(Row row);
}
