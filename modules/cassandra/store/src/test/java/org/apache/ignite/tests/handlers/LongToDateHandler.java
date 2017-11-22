package org.apache.ignite.tests.handlers;

import com.datastax.driver.core.Row;
import org.apache.ignite.cache.store.cassandra.common.TypeHandler;

import java.util.Date;

public class LongToDateHandler implements TypeHandler<Long, Date> {
    @Override
    public Long toJavaType(Row row, int index) {
        if (row.isNull(index)) {
            return null;
        }
        return row.getTimestamp(index).getTime();
    }

    @Override
    public Long toJavaType(Row row, String col) {
        if (row.isNull(col)) {
            return null;
        }
        return row.getTimestamp(col).getTime();
    }

    @Override
    public Date toCassandraPrimitiveType(Long javaValue) {
        if (javaValue == null) {
            return null;
        }
        return new Date(javaValue);
    }

    @Override
    public Class<Date> getClazz() {
        return Date.class;
    }
}
