package org.apache.ignite.cache.store.cassandra.handler;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;

import java.util.Date;

/**
 * Type handler for convert java {@link Date} &lt;-&gt; cassandra 'timestamp'
 */
public class DateTypeHandler implements TypeHandler<Date, Date> {

    /**
     * {@inheritDoc}
     */
    @Override
    public Date toJavaType(Row row, int index) {
        return row.getTimestamp(index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Date toJavaType(Row row, String col) {
        return row.getTimestamp(col);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Date toCassandraPrimitiveType(Date javaValue) {
        return javaValue;
    }

    /**
     * Get 'timestamp' cassandra type.
     * @return 'timestamp' cassandra type
     */
    @Override
    public String getDDLType() {
        return DataType.Name.TIMESTAMP.toString();
    }
}
