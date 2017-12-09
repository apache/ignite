package org.apache.ignite.cache.store.cassandra.handler;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;

/**
 * Type handler for convert java {@link Double} &lt;-&gt; cassandra 'double'
 */
public class DoubleTypeHandler implements TypeHandler<Double, Double> {

    /**
     * {@inheritDoc}
     */
    @Override
    public Double toJavaType(Row row, int index) {
        if(row.isNull(index)) {
            return null;
        }
        return row.getDouble(index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Double toJavaType(Row row, String col) {
        if(row.isNull(col)) {
            return null;
        }
        return row.getDouble(col);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Double toCassandraPrimitiveType(Double javaValue) {
        return javaValue;
    }

    /**
     * Get 'double' cassandra type.
     * @return 'double' cassandra type
     */
    @Override
    public String getDDLType() {
        return DataType.Name.DOUBLE.toString();
    }
}
