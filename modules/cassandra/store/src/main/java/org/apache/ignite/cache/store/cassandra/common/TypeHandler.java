package org.apache.ignite.cache.store.cassandra.common;

import com.datastax.driver.core.Row;

public interface TypeHandler<J, C> {
    J toJavaType(Row row, int index);
    J toJavaType(Row row, String col);
    C toCassandraPrimitiveType(J javaValue);
    Class<C> getClazz();
}
