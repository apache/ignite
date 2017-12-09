package org.apache.ignite.cache.store.cassandra.handler;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;

/**
 * Type handler for convert java {@link Boolean} &lt;-&gt; cassandra 'boolean'
 */
public class BooleanTypeHandler implements TypeHandler<Boolean, Boolean> {

    /**
     * {@inheritDoc}
     */
    @Override
    public Boolean toJavaType(Row row, int index) {
        if(row.isNull(index)) {
            return null;
        }
        return row.getBool(index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Boolean toJavaType(Row row, String col) {
        if(row.isNull(col)) {
            return null;
        }
        return row.getBool(col);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Boolean toCassandraPrimitiveType(Boolean javaValue) {
        return javaValue;
    }

    /**
     * Get 'boolean' cassandra type.
     * @return 'boolean' cassandra type
     */
    @Override
    public String getDDLType() {
        return DataType.Name.BOOLEAN.toString();
    }
}
