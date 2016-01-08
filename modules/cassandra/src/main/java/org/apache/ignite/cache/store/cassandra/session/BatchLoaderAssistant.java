package org.apache.ignite.cache.store.cassandra.session;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;

/**
 * Provides information for loadCache operation of {@link org.apache.ignite.cache.store.cassandra.CassandraCacheStore}.
 */
public interface BatchLoaderAssistant {
    /**
     * Returns name of the batch load operation.
     *
     * @return operation name.
     */
    public String operationName();

    /**
     * Returns CQL statement to use in batch load operation.
     *
     * @return CQL statement for batch load operation.
     */
    public Statement getStatement();

    /**
     * Processes each row returned by batch load operation.
     *
     * @param row row selected from Cassandra table.
     */
    public void process(Row row);
}
