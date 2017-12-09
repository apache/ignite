package org.apache.ignite.tests.handlers;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import org.apache.ignite.cache.store.cassandra.handler.TypeHandler;

public class StringToLongTestTypeHandler implements TypeHandler<String, Long> {
    @Override
    public String toJavaType(Row row, int index) {
        if (row.isNull(index)) {
            return null;
        }
        return String.valueOf(row.getLong(index));
    }

    @Override
    public String toJavaType(Row row, String col) {
        if (row.isNull(col)) {
            return null;
        }
        return String.valueOf(row.getLong(col));
    }

    @Override
    public Long toCassandraPrimitiveType(String javaValue) {
        if (javaValue == null) {
            return null;
        }
        return Long.parseLong(javaValue);
    }

    @Override
    public String getDDLType() {
        return DataType.Name.BIGINT.toString();
    }

}
