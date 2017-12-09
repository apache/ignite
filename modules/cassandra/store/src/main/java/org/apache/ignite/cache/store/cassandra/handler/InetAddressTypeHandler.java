package org.apache.ignite.cache.store.cassandra.handler;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;

import java.net.InetAddress;

/**
 * Type handler for convert java {@link InetAddress} &lt;-&gt; cassandra 'inet'
 */
public class InetAddressTypeHandler implements TypeHandler<InetAddress, InetAddress> {

    /**
     * {@inheritDoc}
     */
    @Override
    public InetAddress toJavaType(Row row, int index) {
        return row.getInet(index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InetAddress toJavaType(Row row, String col) {
        return row.getInet(col);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InetAddress toCassandraPrimitiveType(InetAddress javaValue) {
        return javaValue;
    }

    /**
     * Get 'inet' cassandra type.
     * @return 'inet' cassandra type
     */
    @Override
    public String getDDLType() {
        return DataType.Name.INET.toString();
    }
}
