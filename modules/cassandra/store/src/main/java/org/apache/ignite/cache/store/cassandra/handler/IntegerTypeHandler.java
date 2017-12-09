package org.apache.ignite.cache.store.cassandra.handler;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;

/**
 * Type handler for convert java {@link Integer} &lt;-&gt; cassandra 'int'
 */
public class IntegerTypeHandler implements TypeHandler<Integer, Integer> {

    /**
     * {@inheritDoc}
     */
    @Override
    public Integer toJavaType(Row row, int index) {
        if(row.isNull(index)) {
            return null;
        }
        return row.getInt(index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Integer toJavaType(Row row, String col) {
        if(row.isNull(col)) {
            return null;
        }
        return row.getInt(col);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Integer toCassandraPrimitiveType(Integer javaValue) {
        return javaValue;
    }

    /**
     * Get 'int' cassandra type.
     * @return 'int' cassandra type
     */
    @Override
    public String getDDLType() {
        return DataType.Name.INT.toString();
    }
}
