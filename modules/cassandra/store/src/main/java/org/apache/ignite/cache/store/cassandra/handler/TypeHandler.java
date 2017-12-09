package org.apache.ignite.cache.store.cassandra.handler;

import com.datastax.driver.core.Row;

/**
 * Interface which should be implemented by all handlers responsible
 * for converting data to/from primitive Cassandra type.
 */
public interface TypeHandler<J, C> {
    /**
     * Get primitive Cassandra type object from database and convert to complex Java type object
     *
     * @param row row from Cassandra
     * @param index index column in row
     * @return data from Cassandra converted to complex java type object
     */
    J toJavaType(Row row, int index);

    /**
     * Get primitive Cassandra type object from database and convert to complex Java type object
     *
     * @param row row from Cassandra
     * @param col name column in row
     * @return data from Cassandra converted to complex java type object
     */
    J toJavaType(Row row, String col);

    /**
     * Convert complex Java type object to primitive Cassandra type object
     *
     * @param javaValue complex Java type object
     * @return data converted to a primitive Cassandra type object
     */
    C toCassandraPrimitiveType(J javaValue);

    /**
     * Get DDL for cassandra type.
     *
     * @return DDL type
     */
    String getDDLType();
}
