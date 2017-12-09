package org.apache.ignite.cache.store.cassandra.handler;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;

import java.math.BigInteger;

/**
 * Type handler for convert java {@link BigInteger} &lt;-&gt; cassandra 'varint'
 */
public class BigIntegerTypeHandler implements TypeHandler<BigInteger, BigInteger> {

    /**
     * {@inheritDoc}
     */
    @Override
    public BigInteger toJavaType(Row row, int index) {
        return row.getVarint(index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BigInteger toJavaType(Row row, String col) {
        return row.getVarint(col);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BigInteger toCassandraPrimitiveType(BigInteger javaValue) {
        return javaValue;
    }

    /**
     * Get 'varint' cassandra type.
     * @return 'varint' cassandra type
     */
    @Override
    public String getDDLType() {
        return DataType.Name.VARINT.toString();
    }
}
