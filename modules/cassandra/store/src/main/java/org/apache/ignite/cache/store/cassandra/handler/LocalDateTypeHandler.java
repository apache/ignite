package org.apache.ignite.cache.store.cassandra.handler;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.Row;

/**
 * Type handler for convert java {@link com.datastax.driver.core.LocalDate} &lt;-&gt; cassandra 'date'
 */
public class LocalDateTypeHandler implements TypeHandler<LocalDate, LocalDate> {

    /**
     * {@inheritDoc}
     */
    @Override
    public LocalDate toJavaType(Row row, int index) {
        return row.getDate(index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LocalDate toJavaType(Row row, String col) {
        return row.getDate(col);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LocalDate toCassandraPrimitiveType(LocalDate javaValue) {
        return javaValue;
    }

    /**
     * Get 'date' cassandra type.
     * @return 'date' cassandra type
     */
    @Override
    public String getDDLType() {
        return DataType.Name.DATE.toString();
    }
}
