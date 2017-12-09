package org.apache.ignite.cache.store.cassandra.handler;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;

import java.util.UUID;

/**
 * Type handler for convert java {@link UUID} &lt;-&gt; cassandra 'uuid'
 */
public class UUIDTypeHandler implements TypeHandler<UUID, UUID> {

    /**
     * {@inheritDoc}
     */
    @Override
    public UUID toJavaType(Row row, int index) {
        return row.getUUID(index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UUID toJavaType(Row row, String col) {
        return row.getUUID(col);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UUID toCassandraPrimitiveType(UUID javaValue) {
        return javaValue;
    }

    /**
     * Get 'uuid' cassandra type.
     * @return 'uuid' cassandra type
     */
    @Override
    public String getDDLType() {
        return DataType.Name.UUID.toString();
    }
}
