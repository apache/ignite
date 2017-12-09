package org.apache.ignite.cache.store.cassandra.handler;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;

/**
 * Type handler for convert java {@link Long} &lt;-&gt; cassandra 'date'
 */
public class LongTypeHandler implements TypeHandler<Long, Long> {

    /**
     * {@inheritDoc}
     */
    @Override
    public Long toJavaType(Row row, int index) {
        if(row.isNull(index)) {
            return null;
        }
        return row.getLong(index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Long toJavaType(Row row, String col) {
        if(row.isNull(col)) {
            return null;
        }
        return row.getLong(col);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Long toCassandraPrimitiveType(Long javaValue) {
        return javaValue;
    }

    /**
     * Get 'bigint' cassandra type.
     * @return 'bigint' cassandra type
     */
    @Override
    public String getDDLType() {
        return DataType.Name.BIGINT.toString();
    }
}
