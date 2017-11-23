package org.apache.ignite.tests.handlers;

import com.datastax.driver.core.Row;
import org.apache.ignite.cache.store.cassandra.common.TypeHandler;

public class LongToStringHandler implements TypeHandler<Long, String> {
    @Override
    public Long toJavaType(Row row, int index) {
        if (row.isNull(index)) {
            return null;
        }
        return Long.parseLong(row.getString(index));
    }

    @Override
    public Long toJavaType(Row row, String col) {
        if (row.isNull(col)) {
            return null;
        }
        return Long.parseLong(row.getString(col));
    }

    @Override
    public String toCassandraPrimitiveType(Long javaValue) {
        if (javaValue == null) {
            return null;
        }
        return String.valueOf(javaValue);
    }

    @Override
    public Class<String> getClazz() {
        return String.class;
    }
}
