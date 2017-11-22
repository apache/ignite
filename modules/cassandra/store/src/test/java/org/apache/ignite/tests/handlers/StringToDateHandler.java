package org.apache.ignite.tests.handlers;

import com.datastax.driver.core.Row;
import org.apache.ignite.cache.store.cassandra.common.TypeHandler;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class StringToDateHandler implements TypeHandler<String, Date> {
    private static final DateFormat DATE_FORMAT = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");

    @Override
    public String toJavaType(Row row, int index) {
        if (row.isNull(index)) {
            return null;
        }
        return DATE_FORMAT.format(row.getTimestamp(index));
    }

    @Override
    public String toJavaType(Row row, String col) {
        if (row.isNull(col)) {
            return null;
        }
        return DATE_FORMAT.format(row.getTimestamp(col));
    }

    @Override
    public Date toCassandraPrimitiveType(String javaValue) {
        if (javaValue == null) {
            return null;
        }
        return parse(javaValue);
    }

    @Override
    public Class<Date> getClazz() {
        return Date.class;
    }

    private Date parse(String value) {
        try {
            return DATE_FORMAT.parse(value);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }
}
