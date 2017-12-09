package org.apache.ignite.cache.store.cassandra.handler;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;

/**
 * Type handler for convert java {@link String} &lt;-&gt; cassandra 'text'
 */
public class StringTypeHandler implements TypeHandler<String, String> {

    /**
     * {@inheritDoc}
     */
    @Override
    public String toJavaType(Row row, int index) {
        return row.getString(index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toJavaType(Row row, String col) {
        return row.getString(col);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toCassandraPrimitiveType(String javaValue) {
        return javaValue;
    }

    /**
     * Get 'text' cassandra type.
     * @return 'text' cassandra type
     */
    @Override
    public String getDDLType() {
        return DataType.Name.TEXT.toString();
    }
}
