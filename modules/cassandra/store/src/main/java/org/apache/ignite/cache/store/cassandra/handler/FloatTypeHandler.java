package org.apache.ignite.cache.store.cassandra.handler;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;

/**
 * Type handler for convert java {@link Float} &lt;-&gt; cassandra 'float'
 */
public class FloatTypeHandler implements TypeHandler<Float, Float> {

    /**
     * {@inheritDoc}
     */
    @Override
    public Float toJavaType(Row row, int index) {
        if(row.isNull(index)) {
            return null;
        }
        return row.getFloat(index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Float toJavaType(Row row, String col) {
        if(row.isNull(col)) {
            return null;
        }
        return row.getFloat(col);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Float toCassandraPrimitiveType(Float javaValue) {
        return javaValue;
    }

    /**
     * Get 'float' cassandra type.
     * @return 'float' cassandra type
     */
    @Override
    public String getDDLType() {
        return DataType.Name.FLOAT.toString();
    }
}
