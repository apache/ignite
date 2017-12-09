package org.apache.ignite.cache.store.cassandra.handler;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;

import java.nio.ByteBuffer;

/**
 * Type handler for convert java byte[] &lt;-&gt; cassandra 'blob'
 */
public class ByteArrayTypeHandler implements TypeHandler<byte[], ByteBuffer> {

    /**
     * {@inheritDoc}
     */
    @Override
    public byte[] toJavaType(Row row, int index) {
        if(row.isNull(index)) {
            return null;
        }
        return row.getBytes(index).array();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public byte[] toJavaType(Row row, String col) {
        if(row.isNull(col)) {
            return null;
        }
        return row.getBytes(col).array();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ByteBuffer toCassandraPrimitiveType(byte[] javaValue) {
        if(javaValue == null) {
            return null;
        }
        return ByteBuffer.wrap(javaValue);
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
