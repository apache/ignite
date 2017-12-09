package org.apache.ignite.cache.store.cassandra.handler;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;

import java.math.BigDecimal;

/**
 * Type handler for convert java {@link BigDecimal} &lt;-&gt; cassandra 'decimal'
 */
public class BigDecimalTypeHandler implements TypeHandler<BigDecimal, BigDecimal> {

    /**
     * {@inheritDoc}
     */
    @Override
    public BigDecimal toJavaType(Row row, int index) {
        return row.getDecimal(index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BigDecimal toJavaType(Row row, String col) {
        return row.getDecimal(col);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BigDecimal toCassandraPrimitiveType(BigDecimal javaValue) {
        return javaValue;
    }

    /**
     * Get 'decimal' cassandra type.
     * @return 'decimal' cassandra type
     */
    @Override
    public String getDDLType() {
        return DataType.Name.DECIMAL.toString();
    }
}
