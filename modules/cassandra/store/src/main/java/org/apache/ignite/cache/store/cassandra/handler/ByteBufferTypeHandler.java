package org.apache.ignite.cache.store.cassandra.handler;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;

import java.nio.ByteBuffer;

/**
 * Type handler for convert java {@link ByteBuffer} &lt;-&gt; cassandra 'blob'
 */
public class ByteBufferTypeHandler implements TypeHandler<ByteBuffer, ByteBuffer> {

    /**
     * {@inheritDoc}
     */
    @Override
    public ByteBuffer toJavaType(Row row, int index) {
        return row.getBytes(index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ByteBuffer toJavaType(Row row, String col) {
        return row.getBytes(col);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ByteBuffer toCassandraPrimitiveType(ByteBuffer javaValue) {
        return javaValue;
    }

    /**
     * Get 'blob' cassandra type.
     * @return 'blob' cassandra type
     */
    @Override
    public String getDDLType() {
        return DataType.Name.BLOB.toString();
    }
}
