/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.UUID;

import org.h2.api.ErrorCode;
import org.h2.api.Interval;
import org.h2.api.IntervalQualifier;
import org.h2.api.TimestampWithTimeZone;
import org.h2.engine.Mode;
import org.h2.engine.SessionInterface;
import org.h2.engine.SysProperties;
import org.h2.jdbc.JdbcArray;
import org.h2.jdbc.JdbcBlob;
import org.h2.jdbc.JdbcClob;
import org.h2.jdbc.JdbcConnection;
import org.h2.jdbc.JdbcLob;
import org.h2.message.DbException;
import org.h2.util.JdbcUtils;
import org.h2.util.LocalDateTimeUtils;
import org.h2.util.Utils;

/**
 * This class contains meta data information about data types,
 * and can convert between Java objects and Values.
 */
public class DataType {

    /**
     * This constant is used to represent the type of a ResultSet. There is no
     * equivalent java.sql.Types value, but Oracle uses it to represent a
     * ResultSet (OracleTypes.CURSOR = -10).
     */
    public static final int TYPE_RESULT_SET = -10;

    /**
     * The Geometry class. This object is null if the jts jar file is not in the
     * classpath.
     */
    public static final Class<?> GEOMETRY_CLASS;

    private static final String GEOMETRY_CLASS_NAME =
            "org.locationtech.jts.geom.Geometry";

    /**
     * The list of types. An ArrayList so that Tomcat doesn't set it to null
     * when clearing references.
     */
    private static final ArrayList<DataType> TYPES = new ArrayList<>(96);
    private static final HashMap<String, DataType> TYPES_BY_NAME = new HashMap<>(128);
    /**
     * Mapping from Value type numbers to DataType.
     */
    static final DataType[] TYPES_BY_VALUE_TYPE = new DataType[Value.TYPE_COUNT];

    /**
     * The value type of this data type.
     */
    public int type;

    /**
     * The data type name.
     */
    public String name;

    /**
     * The SQL type.
     */
    public int sqlType;

    /**
     * How closely the data type maps to the corresponding JDBC SQL type (low is
     * best).
     */
    public int sqlTypePos;

    /**
     * The maximum supported precision.
     */
    public long maxPrecision;

    /**
     * The lowest possible scale.
     */
    public int minScale;

    /**
     * The highest possible scale.
     */
    public int maxScale;

    /**
     * If this is a numeric type.
     */
    public boolean decimal;

    /**
     * The prefix required for the SQL literal representation.
     */
    public String prefix;

    /**
     * The suffix required for the SQL literal representation.
     */
    public String suffix;

    /**
     * The list of parameters used in the column definition.
     */
    public String params;

    /**
     * If this is an autoincrement type.
     */
    public boolean autoIncrement;

    /**
     * If this data type is an autoincrement type.
     */
    public boolean caseSensitive;

    /**
     * If the precision parameter is supported.
     */
    public boolean supportsPrecision;

    /**
     * If the scale parameter is supported.
     */
    public boolean supportsScale;

    /**
     * The default precision.
     */
    public long defaultPrecision;

    /**
     * The default scale.
     */
    public int defaultScale;

    /**
     * If this data type should not be listed in the database meta data.
     */
    public boolean hidden;

    static {
        Class<?> g;
        try {
            g = JdbcUtils.loadUserClass(GEOMETRY_CLASS_NAME);
        } catch (Exception e) {
            // class is not in the classpath - ignore
            g = null;
        }
        GEOMETRY_CLASS = g;

        DataType dataType = new DataType();
        dataType.defaultPrecision = dataType.maxPrecision = ValueNull.PRECISION;
        add(Value.NULL, Types.NULL,
                dataType,
                new String[]{"NULL"}
        );
        add(Value.STRING, Types.VARCHAR,
                createString(true),
                new String[]{"VARCHAR", "CHARACTER VARYING", "VARCHAR2", "NVARCHAR", "NVARCHAR2",
                    "VARCHAR_CASESENSITIVE", "TID"}
        );
        add(Value.STRING, Types.LONGVARCHAR,
                createString(true),
                new String[]{"LONGVARCHAR", "LONGNVARCHAR"}
        );
        add(Value.STRING_FIXED, Types.CHAR,
                createString(true),
                new String[]{"CHAR", "CHARACTER", "NCHAR"}
        );
        add(Value.STRING_IGNORECASE, Types.VARCHAR,
                createString(false),
                new String[]{"VARCHAR_IGNORECASE"}
        );
        add(Value.BOOLEAN, Types.BOOLEAN,
                createNumeric(ValueBoolean.PRECISION, 0, false),
                new String[]{"BOOLEAN", "BIT", "BOOL"}
        );
        add(Value.BYTE, Types.TINYINT,
                createNumeric(ValueByte.PRECISION, 0, false),
                new String[]{"TINYINT"}
        );
        add(Value.SHORT, Types.SMALLINT,
                createNumeric(ValueShort.PRECISION, 0, false),
                new String[]{"SMALLINT", "YEAR", "INT2"}
        );
        add(Value.INT, Types.INTEGER,
                createNumeric(ValueInt.PRECISION, 0, false),
                new String[]{"INTEGER", "INT", "MEDIUMINT", "INT4", "SIGNED"}
        );
        add(Value.INT, Types.INTEGER,
                createNumeric(ValueInt.PRECISION, 0, true),
                new String[]{"SERIAL"}
        );
        add(Value.LONG, Types.BIGINT,
                createNumeric(ValueLong.PRECISION, 0, false),
                new String[]{"BIGINT", "INT8", "LONG"}
        );
        add(Value.LONG, Types.BIGINT,
                createNumeric(ValueLong.PRECISION, 0, true),
                new String[]{"IDENTITY", "BIGSERIAL"}
        );
        if (SysProperties.BIG_DECIMAL_IS_DECIMAL) {
            addDecimal();
            addNumeric();
        } else {
            addNumeric();
            addDecimal();
        }
        add(Value.FLOAT, Types.REAL,
                createNumeric(ValueFloat.PRECISION, 0, false),
                new String[] {"REAL", "FLOAT4"}
        );
        add(Value.DOUBLE, Types.DOUBLE,
                createNumeric(ValueDouble.PRECISION, 0, false),
                new String[] { "DOUBLE", "DOUBLE PRECISION" }
        );
        add(Value.DOUBLE, Types.FLOAT,
                createNumeric(ValueDouble.PRECISION, 0, false),
                new String[] {"FLOAT", "FLOAT8" }
        );
        add(Value.TIME, Types.TIME,
                createDate(ValueTime.MAXIMUM_PRECISION, ValueTime.DEFAULT_PRECISION,
                        "TIME", true, ValueTime.DEFAULT_SCALE, ValueTime.MAXIMUM_SCALE),
                new String[]{"TIME", "TIME WITHOUT TIME ZONE"}
        );
        add(Value.DATE, Types.DATE,
                createDate(ValueDate.PRECISION, ValueDate.PRECISION,
                        "DATE", false, 0, 0),
                new String[]{"DATE"}
        );
        add(Value.TIMESTAMP, Types.TIMESTAMP,
                createDate(ValueTimestamp.MAXIMUM_PRECISION, ValueTimestamp.DEFAULT_PRECISION,
                        "TIMESTAMP", true, ValueTimestamp.DEFAULT_SCALE, ValueTimestamp.MAXIMUM_SCALE),
                new String[]{"TIMESTAMP", "TIMESTAMP WITHOUT TIME ZONE",
                        "DATETIME", "DATETIME2", "SMALLDATETIME"}
        );
        // 2014 is the value of Types.TIMESTAMP_WITH_TIMEZONE
        // use the value instead of the reference because the code has to
        // compile (on Java 1.7). Can be replaced with
        // Types.TIMESTAMP_WITH_TIMEZONE once Java 1.8 is required.
        add(Value.TIMESTAMP_TZ, 2014,
                createDate(ValueTimestampTimeZone.MAXIMUM_PRECISION, ValueTimestampTimeZone.DEFAULT_PRECISION,
                        "TIMESTAMP_TZ", true, ValueTimestampTimeZone.DEFAULT_SCALE,
                        ValueTimestampTimeZone.MAXIMUM_SCALE),
                new String[]{"TIMESTAMP WITH TIME ZONE"}
        );
        add(Value.BYTES, Types.VARBINARY,
                createString(false),
                new String[]{"VARBINARY", "BINARY VARYING"}
        );
        add(Value.BYTES, Types.BINARY,
                createString(false),
                new String[]{"BINARY", "RAW", "BYTEA", "LONG RAW"}
        );
        add(Value.BYTES, Types.LONGVARBINARY,
                createString(false),
                new String[]{"LONGVARBINARY"}
        );
        dataType = new DataType();
        dataType.prefix = dataType.suffix = "'";
        dataType.defaultPrecision = dataType.maxPrecision = ValueUuid.PRECISION;
        add(Value.UUID, Types.BINARY,
                createString(false),
                // UNIQUEIDENTIFIER is the MSSQL mode equivalent
                new String[]{"UUID", "UNIQUEIDENTIFIER"}
        );
        add(Value.JAVA_OBJECT, Types.OTHER,
                createString(false),
                new String[]{"OTHER", "OBJECT", "JAVA_OBJECT"}
        );
        add(Value.BLOB, Types.BLOB,
                createLob(),
                new String[]{"BLOB", "BINARY LARGE OBJECT", "TINYBLOB", "MEDIUMBLOB",
                    "LONGBLOB", "IMAGE", "OID"}
        );
        add(Value.CLOB, Types.CLOB,
                createLob(),
                new String[]{"CLOB", "CHARACTER LARGE OBJECT", "TINYTEXT", "TEXT", "MEDIUMTEXT",
                    "LONGTEXT", "NTEXT", "NCLOB"}
        );
        add(Value.GEOMETRY, Types.OTHER,
                createGeometry(),
                new String[]{"GEOMETRY"}
        );
        dataType = new DataType();
        dataType.prefix = "ARRAY[";
        dataType.suffix = "]";
        add(Value.ARRAY, Types.ARRAY,
                dataType,
                new String[]{"ARRAY"}
        );
        dataType = new DataType();
        dataType.maxPrecision = dataType.defaultPrecision = Integer.MAX_VALUE;
        add(Value.RESULT_SET, DataType.TYPE_RESULT_SET,
                dataType,
                new String[]{"RESULT_SET"}
        );
        dataType = createString(false);
        dataType.supportsPrecision = false;
        dataType.supportsScale = false;
        add(Value.ENUM, Types.OTHER,
                dataType,
                new String[]{"ENUM"}
        );
        for (int i = Value.INTERVAL_YEAR; i <= Value.INTERVAL_MINUTE_TO_SECOND; i++) {
            addInterval(i);
        }
        // Row value doesn't have a type name
        dataType = new DataType();
        dataType.type = Value.ROW;
        dataType.name = "ROW";
        dataType.sqlType = Types.OTHER;
        dataType.prefix = "ROW(";
        dataType.suffix = ")";
        TYPES_BY_VALUE_TYPE[Value.ROW] = dataType;
    }

    private static void addDecimal() {
        add(Value.DECIMAL, Types.DECIMAL,
                createNumeric(Integer.MAX_VALUE, ValueDecimal.DEFAULT_PRECISION, ValueDecimal.DEFAULT_SCALE),
                new String[]{"DECIMAL", "DEC"}
        );
    }

    private static void addNumeric() {
        add(Value.DECIMAL, Types.NUMERIC,
                createNumeric(Integer.MAX_VALUE, ValueDecimal.DEFAULT_PRECISION, ValueDecimal.DEFAULT_SCALE),
                new String[]{"NUMERIC", "NUMBER"}
        );
    }

    private static void addInterval(int type) {
        IntervalQualifier qualifier = IntervalQualifier.valueOf(type - Value.INTERVAL_YEAR);
        String name = qualifier.toString();
        DataType dataType = new DataType();
        dataType.prefix = "INTERVAL ";
        dataType.suffix = ' ' + name;
        dataType.supportsPrecision = true;
        dataType.defaultPrecision = ValueInterval.DEFAULT_PRECISION;
        dataType.maxPrecision = ValueInterval.MAXIMUM_PRECISION;
        if (qualifier.hasSeconds()) {
            dataType.supportsScale = true;
            dataType.defaultScale = ValueInterval.DEFAULT_SCALE;
            dataType.maxScale = ValueInterval.MAXIMUM_SCALE;
            dataType.params = "PRECISION,SCALE";
        } else {
            dataType.params = "PRECISION";
        }
        add(type, Types.OTHER, dataType,
                new String[]{("INTERVAL " + name).intern()}
        );
    }

    private static void add(int type, int sqlType,
            DataType dataType, String[] names) {
        for (int i = 0; i < names.length; i++) {
            DataType dt = new DataType();
            dt.type = type;
            dt.sqlType = sqlType;
            dt.name = names[i];
            dt.autoIncrement = dataType.autoIncrement;
            dt.decimal = dataType.decimal;
            dt.maxPrecision = dataType.maxPrecision;
            dt.maxScale = dataType.maxScale;
            dt.minScale = dataType.minScale;
            dt.params = dataType.params;
            dt.prefix = dataType.prefix;
            dt.suffix = dataType.suffix;
            dt.supportsPrecision = dataType.supportsPrecision;
            dt.supportsScale = dataType.supportsScale;
            dt.defaultPrecision = dataType.defaultPrecision;
            dt.defaultScale = dataType.defaultScale;
            dt.caseSensitive = dataType.caseSensitive;
            dt.hidden = i > 0;
            for (DataType t2 : TYPES) {
                if (t2.sqlType == dt.sqlType) {
                    dt.sqlTypePos++;
                }
            }
            TYPES_BY_NAME.put(dt.name, dt);
            if (TYPES_BY_VALUE_TYPE[type] == null) {
                TYPES_BY_VALUE_TYPE[type] = dt;
            }
            TYPES.add(dt);
        }
    }

    /**
     * Create a width numeric data type without parameters.
     *
     * @param precision precision
     * @param scale scale
     * @param autoInc whether the data type is an auto-increment type
     * @return data type
     */
    public static DataType createNumeric(int precision, int scale, boolean autoInc) {
        DataType dataType = new DataType();
        dataType.defaultPrecision = dataType.maxPrecision = precision;
        dataType.defaultScale = dataType.maxScale = dataType.minScale = scale;
        dataType.decimal = true;
        dataType.autoIncrement = autoInc;
        return dataType;
    }

    /**
     * Create a numeric data type.
     *
     * @param maxPrecision maximum supported precision
     * @param defaultPrecision default precision
     * @param defaultScale default scale
     * @return data type
     */
    public static DataType createNumeric(int maxPrecision, int defaultPrecision, int defaultScale) {
        DataType dataType = new DataType();
        dataType.maxPrecision = maxPrecision;
        dataType.defaultPrecision = defaultPrecision;
        dataType.defaultScale = defaultScale;
        dataType.params = "PRECISION,SCALE";
        dataType.supportsPrecision = true;
        dataType.supportsScale = true;
        dataType.maxScale = maxPrecision;
        dataType.decimal = true;
        return dataType;
    }

    /**
     * Create a date-time data type.
     *
     * @param maxPrecision maximum supported precision
     * @param precision default precision
     * @param prefix the prefix for SQL literal representation
     * @param supportsScale whether the scale parameter is supported
     * @param scale default scale
     * @param maxScale highest possible scale
     * @return data type
     */
    public static DataType createDate(int maxPrecision, int precision, String prefix,
            boolean supportsScale, int scale, int maxScale) {
        DataType dataType = new DataType();
        dataType.prefix = prefix + " '";
        dataType.suffix = "'";
        dataType.maxPrecision = maxPrecision;
        dataType.defaultPrecision = precision;
        if (supportsScale) {
            dataType.params = "SCALE";
            dataType.supportsScale = true;
            dataType.maxScale = maxScale;
            dataType.defaultScale = scale;
        }
        return dataType;
    }

    private static DataType createString(boolean caseSensitive) {
        DataType dataType = new DataType();
        dataType.prefix = "'";
        dataType.suffix = "'";
        dataType.params = "LENGTH";
        dataType.caseSensitive = caseSensitive;
        dataType.supportsPrecision = true;
        dataType.maxPrecision = Integer.MAX_VALUE;
        dataType.defaultPrecision = Integer.MAX_VALUE;
        return dataType;
    }

    private static DataType createLob() {
        DataType t = createString(true);
        t.maxPrecision = Long.MAX_VALUE;
        t.defaultPrecision = Long.MAX_VALUE;
        return t;
    }

    private static DataType createGeometry() {
        DataType dataType = new DataType();
        dataType.prefix = "'";
        dataType.suffix = "'";
        dataType.params = "TYPE,SRID";
        dataType.maxPrecision = Integer.MAX_VALUE;
        dataType.defaultPrecision = Integer.MAX_VALUE;
        return dataType;
    }

    /**
     * Get the list of data types.
     *
     * @return the list
     */
    public static ArrayList<DataType> getTypes() {
        return TYPES;
    }

    /**
     * Read a value from the given result set.
     *
     * @param session the session
     * @param rs the result set
     * @param columnIndex the column index (1 based)
     * @param type the data type
     * @return the value
     */
    public static Value readValue(SessionInterface session, ResultSet rs,
            int columnIndex, int type) {
        try {
            Value v;
            switch (type) {
            case Value.NULL: {
                return ValueNull.INSTANCE;
            }
            case Value.BYTES: {
                /*
                 * Both BINARY and UUID may be mapped to Value.BYTES. getObject() returns byte[]
                 * for SQL BINARY, UUID for SQL UUID and null for SQL NULL.
                 */
                Object o = rs.getObject(columnIndex);
                if (o instanceof byte[]) {
                    v = ValueBytes.getNoCopy((byte[]) o);
                } else if (o != null) {
                    v = ValueUuid.get((UUID) o);
                } else {
                    v = ValueNull.INSTANCE;
                }
                break;
            }
            case Value.UUID: {
                Object o = rs.getObject(columnIndex);
                if (o instanceof UUID) {
                    v = ValueUuid.get((UUID) o);
                } else if (o != null) {
                    v = ValueUuid.get((byte[]) o);
                } else {
                    v = ValueNull.INSTANCE;
                }
                break;
            }
            case Value.BOOLEAN: {
                boolean value = rs.getBoolean(columnIndex);
                v = rs.wasNull() ? (Value) ValueNull.INSTANCE :
                    ValueBoolean.get(value);
                break;
            }
            case Value.BYTE: {
                byte value = rs.getByte(columnIndex);
                v = rs.wasNull() ? (Value) ValueNull.INSTANCE :
                    ValueByte.get(value);
                break;
            }
            case Value.DATE: {
                Date value = rs.getDate(columnIndex);
                v = value == null ? (Value) ValueNull.INSTANCE :
                    ValueDate.get(value);
                break;
            }
            case Value.TIME: {
                Time value = rs.getTime(columnIndex);
                v = value == null ? (Value) ValueNull.INSTANCE :
                    ValueTime.get(value);
                break;
            }
            case Value.TIMESTAMP: {
                Timestamp value = rs.getTimestamp(columnIndex);
                v = value == null ? (Value) ValueNull.INSTANCE :
                    ValueTimestamp.get(value);
                break;
            }
            case Value.TIMESTAMP_TZ: {
                Object obj = rs.getObject(columnIndex);
                if (obj == null) {
                    v = ValueNull.INSTANCE;
                } else if (LocalDateTimeUtils.isJava8DateApiPresent()
                        && LocalDateTimeUtils.OFFSET_DATE_TIME.isInstance(obj)) {
                    v = LocalDateTimeUtils.offsetDateTimeToValue(obj);
                } else {
                    TimestampWithTimeZone value = (TimestampWithTimeZone) obj;
                    v = ValueTimestampTimeZone.get(value);
                }
                break;
            }
            case Value.DECIMAL: {
                BigDecimal value = rs.getBigDecimal(columnIndex);
                v = value == null ? (Value) ValueNull.INSTANCE :
                    ValueDecimal.get(value);
                break;
            }
            case Value.DOUBLE: {
                double value = rs.getDouble(columnIndex);
                v = rs.wasNull() ? (Value) ValueNull.INSTANCE :
                    ValueDouble.get(value);
                break;
            }
            case Value.FLOAT: {
                float value = rs.getFloat(columnIndex);
                v = rs.wasNull() ? (Value) ValueNull.INSTANCE :
                    ValueFloat.get(value);
                break;
            }
            case Value.INT: {
                int value = rs.getInt(columnIndex);
                v = rs.wasNull() ? (Value) ValueNull.INSTANCE :
                    ValueInt.get(value);
                break;
            }
            case Value.LONG: {
                long value = rs.getLong(columnIndex);
                v = rs.wasNull() ? (Value) ValueNull.INSTANCE :
                    ValueLong.get(value);
                break;
            }
            case Value.SHORT: {
                short value = rs.getShort(columnIndex);
                v = rs.wasNull() ? (Value) ValueNull.INSTANCE :
                    ValueShort.get(value);
                break;
            }
            case Value.STRING_IGNORECASE: {
                String s = rs.getString(columnIndex);
                v = (s == null) ? (Value) ValueNull.INSTANCE :
                    ValueStringIgnoreCase.get(s);
                break;
            }
            case Value.STRING_FIXED: {
                String s = rs.getString(columnIndex);
                v = (s == null) ? (Value) ValueNull.INSTANCE :
                    ValueStringFixed.get(s);
                break;
            }
            case Value.STRING: {
                String s = rs.getString(columnIndex);
                v = (s == null) ? (Value) ValueNull.INSTANCE :
                    ValueString.get(s);
                break;
            }
            case Value.CLOB: {
                if (session == null) {
                    String s = rs.getString(columnIndex);
                    v = s == null ? ValueNull.INSTANCE :
                        ValueLobDb.createSmallLob(Value.CLOB, s.getBytes(StandardCharsets.UTF_8));
                } else {
                    Reader in = rs.getCharacterStream(columnIndex);
                    if (in == null) {
                        v = ValueNull.INSTANCE;
                    } else {
                        v = session.getDataHandler().getLobStorage().
                                createClob(new BufferedReader(in), -1);
                    }
                }
                if (session != null) {
                    session.addTemporaryLob(v);
                }
                break;
            }
            case Value.BLOB: {
                if (session == null) {
                    byte[] buff = rs.getBytes(columnIndex);
                    return buff == null ? ValueNull.INSTANCE :
                        ValueLobDb.createSmallLob(Value.BLOB, buff);
                }
                InputStream in = rs.getBinaryStream(columnIndex);
                v = (in == null) ? (Value) ValueNull.INSTANCE :
                    session.getDataHandler().getLobStorage().createBlob(in, -1);
                session.addTemporaryLob(v);
                break;
            }
            case Value.JAVA_OBJECT: {
                if (SysProperties.serializeJavaObject) {
                    byte[] buff = rs.getBytes(columnIndex);
                    v = buff == null ? ValueNull.INSTANCE :
                        ValueJavaObject.getNoCopy(null, buff, session.getDataHandler());
                } else {
                    Object o = rs.getObject(columnIndex);
                    v = o == null ? ValueNull.INSTANCE :
                        ValueJavaObject.getNoCopy(o, null, session.getDataHandler());
                }
                break;
            }
            case Value.ARRAY: {
                Array array = rs.getArray(columnIndex);
                if (array == null) {
                    return ValueNull.INSTANCE;
                }
                Object[] list = (Object[]) array.getArray();
                if (list == null) {
                    return ValueNull.INSTANCE;
                }
                int len = list.length;
                Value[] values = new Value[len];
                for (int i = 0; i < len; i++) {
                    values[i] = DataType.convertToValue(session, list[i], Value.NULL);
                }
                v = ValueArray.get(values);
                break;
            }
            case Value.ENUM: {
                int value = rs.getInt(columnIndex);
                v = rs.wasNull() ? (Value) ValueNull.INSTANCE :
                    ValueInt.get(value);
                break;
            }
            case Value.ROW: {
                Object[] list = (Object[]) rs.getObject(columnIndex);
                if (list == null) {
                    return ValueNull.INSTANCE;
                }
                int len = list.length;
                Value[] values = new Value[len];
                for (int i = 0; i < len; i++) {
                    values[i] = DataType.convertToValue(session, list[i], Value.NULL);
                }
                v = ValueRow.get(values);
                break;
            }
            case Value.RESULT_SET: {
                ResultSet x = (ResultSet) rs.getObject(columnIndex);
                if (x == null) {
                    return ValueNull.INSTANCE;
                }
                return ValueResultSet.get(session, x, Integer.MAX_VALUE);
            }
            case Value.GEOMETRY: {
                Object x = rs.getObject(columnIndex);
                if (x == null) {
                    return ValueNull.INSTANCE;
                }
                return ValueGeometry.getFromGeometry(x);
            }
            case Value.INTERVAL_YEAR:
            case Value.INTERVAL_MONTH:
            case Value.INTERVAL_DAY:
            case Value.INTERVAL_HOUR:
            case Value.INTERVAL_MINUTE:
            case Value.INTERVAL_SECOND:
            case Value.INTERVAL_YEAR_TO_MONTH:
            case Value.INTERVAL_DAY_TO_HOUR:
            case Value.INTERVAL_DAY_TO_MINUTE:
            case Value.INTERVAL_DAY_TO_SECOND:
            case Value.INTERVAL_HOUR_TO_MINUTE:
            case Value.INTERVAL_HOUR_TO_SECOND:
            case Value.INTERVAL_MINUTE_TO_SECOND: {
                Object x = rs.getObject(columnIndex);
                if (x == null) {
                    return ValueNull.INSTANCE;
                }
                Interval interval = (Interval) x;
                return ValueInterval.from(interval.getQualifier(), interval.isNegative(),
                        interval.getLeading(), interval.getRemaining());
            }
            default:
                if (JdbcUtils.customDataTypesHandler != null) {
                    return JdbcUtils.customDataTypesHandler.getValue(type,
                        rs.getObject(columnIndex),
                        session.getDataHandler());
                }
                throw DbException.throwInternalError("type="+type);
            }
            return v;
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
    }

    /**
     * Get the name of the Java class for the given value type.
     *
     * @param type the value type
     * @param forResultSet return mapping for result set
     * @return the class name
     */
    public static String getTypeClassName(int type, boolean forResultSet) {
        switch (type) {
        case Value.BOOLEAN:
            // "java.lang.Boolean";
            return Boolean.class.getName();
        case Value.BYTE:
            if (forResultSet && !SysProperties.OLD_RESULT_SET_GET_OBJECT) {
                // "java.lang.Integer";
                return Integer.class.getName();
            }
            // "java.lang.Byte";
            return Byte.class.getName();
        case Value.SHORT:
            if (forResultSet && !SysProperties.OLD_RESULT_SET_GET_OBJECT) {
                // "java.lang.Integer";
                return Integer.class.getName();
            }
            // "java.lang.Short";
            return Short.class.getName();
        case Value.INT:
            // "java.lang.Integer";
            return Integer.class.getName();
        case Value.LONG:
            // "java.lang.Long";
            return Long.class.getName();
        case Value.DECIMAL:
            // "java.math.BigDecimal";
            return BigDecimal.class.getName();
        case Value.TIME:
            // "java.sql.Time";
            return Time.class.getName();
        case Value.DATE:
            // "java.sql.Date";
            return Date.class.getName();
        case Value.TIMESTAMP:
            // "java.sql.Timestamp";
            return Timestamp.class.getName();
        case Value.TIMESTAMP_TZ:
            if (SysProperties.RETURN_OFFSET_DATE_TIME && LocalDateTimeUtils.isJava8DateApiPresent()) {
                // "java.time.OffsetDateTime";
                return LocalDateTimeUtils.OFFSET_DATE_TIME.getName();
            }
            // "org.h2.api.TimestampWithTimeZone";
            return TimestampWithTimeZone.class.getName();
        case Value.BYTES:
        case Value.UUID:
            // "[B", not "byte[]";
            return byte[].class.getName();
        case Value.STRING:
        case Value.STRING_IGNORECASE:
        case Value.STRING_FIXED:
        case Value.ENUM:
            // "java.lang.String";
            return String.class.getName();
        case Value.BLOB:
            // "java.sql.Blob";
            return java.sql.Blob.class.getName();
        case Value.CLOB:
            // "java.sql.Clob";
            return java.sql.Clob.class.getName();
        case Value.DOUBLE:
            // "java.lang.Double";
            return Double.class.getName();
        case Value.FLOAT:
            // "java.lang.Float";
            return Float.class.getName();
        case Value.NULL:
            return null;
        case Value.JAVA_OBJECT:
            // "java.lang.Object";
            return Object.class.getName();
        case Value.UNKNOWN:
            // anything
            return Object.class.getName();
        case Value.ARRAY:
            return Array.class.getName();
        case Value.RESULT_SET:
            return ResultSet.class.getName();
        case Value.GEOMETRY:
            return GEOMETRY_CLASS != null ? GEOMETRY_CLASS_NAME : String.class.getName();
        case Value.INTERVAL_YEAR:
        case Value.INTERVAL_MONTH:
        case Value.INTERVAL_DAY:
        case Value.INTERVAL_HOUR:
        case Value.INTERVAL_MINUTE:
        case Value.INTERVAL_SECOND:
        case Value.INTERVAL_YEAR_TO_MONTH:
        case Value.INTERVAL_DAY_TO_HOUR:
        case Value.INTERVAL_DAY_TO_MINUTE:
        case Value.INTERVAL_DAY_TO_SECOND:
        case Value.INTERVAL_HOUR_TO_MINUTE:
        case Value.INTERVAL_HOUR_TO_SECOND:
        case Value.INTERVAL_MINUTE_TO_SECOND:
            // "org.h2.api.Interval"
            return Interval.class.getName();
        default:
            if (JdbcUtils.customDataTypesHandler != null) {
                return JdbcUtils.customDataTypesHandler.getDataTypeClassName(type);
            }
            throw DbException.throwInternalError("type="+type);
        }
    }

    /**
     * Get the data type object for the given value type.
     *
     * @param type the value type
     * @return the data type object
     */
    public static DataType getDataType(int type) {
        if (type == Value.UNKNOWN) {
            throw DbException.get(ErrorCode.UNKNOWN_DATA_TYPE_1, "?");
        }
        if (type >= Value.NULL && type < Value.TYPE_COUNT) {
            DataType dt = TYPES_BY_VALUE_TYPE[type];
            if (dt != null) {
                return dt;
            }
        }
        if (JdbcUtils.customDataTypesHandler != null) {
            DataType dt = JdbcUtils.customDataTypesHandler.getDataTypeById(type);
            if (dt != null) {
                return dt;
            }
        }
        return TYPES_BY_VALUE_TYPE[Value.NULL];
    }

    /**
     * Convert a value type to a SQL type.
     *
     * @param type the value type
     * @return the SQL type
     */
    public static int convertTypeToSQLType(int type) {
        return getDataType(type).sqlType;
    }

    /**
     * Convert a SQL type to a value type using SQL type name, in order to
     * manage SQL type extension mechanism.
     *
     * @param sqlType the SQL type
     * @param sqlTypeName the SQL type name
     * @return the value type
     */
    public static int convertSQLTypeToValueType(int sqlType, String sqlTypeName) {
        switch (sqlType) {
            case Types.BINARY:
                if (sqlTypeName.equalsIgnoreCase("UUID")) {
                    return Value.UUID;
                }
                break;
            case Types.OTHER:
            case Types.JAVA_OBJECT:
                if (sqlTypeName.equalsIgnoreCase("geometry")) {
                    return Value.GEOMETRY;
                }
        }
        return convertSQLTypeToValueType(sqlType);
    }

    /**
     * Get the SQL type from the result set meta data for the given column. This
     * method uses the SQL type and type name.
     *
     * @param meta the meta data
     * @param columnIndex the column index (1, 2,...)
     * @return the value type
     */
    public static int getValueTypeFromResultSet(ResultSetMetaData meta,
            int columnIndex) throws SQLException {
        return convertSQLTypeToValueType(
                meta.getColumnType(columnIndex),
                meta.getColumnTypeName(columnIndex));
    }

    /**
     * Convert a SQL type to a value type.
     *
     * @param sqlType the SQL type
     * @return the value type
     */
    public static int convertSQLTypeToValueType(int sqlType) {
        switch (sqlType) {
        case Types.CHAR:
        case Types.NCHAR:
            return Value.STRING_FIXED;
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
        case Types.NVARCHAR:
        case Types.LONGNVARCHAR:
            return Value.STRING;
        case Types.NUMERIC:
        case Types.DECIMAL:
            return Value.DECIMAL;
        case Types.BIT:
        case Types.BOOLEAN:
            return Value.BOOLEAN;
        case Types.INTEGER:
            return Value.INT;
        case Types.SMALLINT:
            return Value.SHORT;
        case Types.TINYINT:
            return Value.BYTE;
        case Types.BIGINT:
            return Value.LONG;
        case Types.REAL:
            return Value.FLOAT;
        case Types.DOUBLE:
        case Types.FLOAT:
            return Value.DOUBLE;
        case Types.BINARY:
        case Types.VARBINARY:
        case Types.LONGVARBINARY:
            return Value.BYTES;
        case Types.OTHER:
        case Types.JAVA_OBJECT:
            return Value.JAVA_OBJECT;
        case Types.DATE:
            return Value.DATE;
        case Types.TIME:
            return Value.TIME;
        case Types.TIMESTAMP:
            return Value.TIMESTAMP;
        case 2014: // Types.TIMESTAMP_WITH_TIMEZONE
            return Value.TIMESTAMP_TZ;
        case Types.BLOB:
            return Value.BLOB;
        case Types.CLOB:
        case Types.NCLOB:
            return Value.CLOB;
        case Types.NULL:
            return Value.NULL;
        case Types.ARRAY:
            return Value.ARRAY;
        case DataType.TYPE_RESULT_SET:
            return Value.RESULT_SET;
        default:
            throw DbException.get(
                    ErrorCode.UNKNOWN_DATA_TYPE_1, Integer.toString(sqlType));
        }
    }

    /**
     * Get the value type for the given Java class.
     *
     * @param x the Java class
     * @return the value type
     */
    public static int getTypeFromClass(Class <?> x) {
        // TODO refactor: too many if/else in functions, can reduce!
        if (x == null || Void.TYPE == x) {
            return Value.NULL;
        }
        if (x.isPrimitive()) {
            x = Utils.getNonPrimitiveClass(x);
        }
        if (String.class == x) {
            return Value.STRING;
        } else if (Integer.class == x) {
            return Value.INT;
        } else if (Long.class == x) {
            return Value.LONG;
        } else if (Boolean.class == x) {
            return Value.BOOLEAN;
        } else if (Double.class == x) {
            return Value.DOUBLE;
        } else if (Byte.class == x) {
            return Value.BYTE;
        } else if (Short.class == x) {
            return Value.SHORT;
        } else if (Character.class == x) {
            throw DbException.get(
                    ErrorCode.DATA_CONVERSION_ERROR_1, "char (not supported)");
        } else if (Float.class == x) {
            return Value.FLOAT;
        } else if (byte[].class == x) {
            return Value.BYTES;
        } else if (UUID.class == x) {
            return Value.UUID;
        } else if (Void.class == x) {
            return Value.NULL;
        } else if (BigDecimal.class.isAssignableFrom(x)) {
            return Value.DECIMAL;
        } else if (ResultSet.class.isAssignableFrom(x)) {
            return Value.RESULT_SET;
        } else if (ValueLobDb.class.isAssignableFrom(x)) {
            return Value.BLOB;
// FIXME no way to distinguish between these 2 types
//        } else if (ValueLobDb.class.isAssignableFrom(x)) {
//            return Value.CLOB;
        } else if (Date.class.isAssignableFrom(x)) {
            return Value.DATE;
        } else if (Time.class.isAssignableFrom(x)) {
            return Value.TIME;
        } else if (Timestamp.class.isAssignableFrom(x)) {
            return Value.TIMESTAMP;
        } else if (java.util.Date.class.isAssignableFrom(x)) {
            return Value.TIMESTAMP;
        } else if (java.io.Reader.class.isAssignableFrom(x)) {
            return Value.CLOB;
        } else if (java.sql.Clob.class.isAssignableFrom(x)) {
            return Value.CLOB;
        } else if (java.io.InputStream.class.isAssignableFrom(x)) {
            return Value.BLOB;
        } else if (java.sql.Blob.class.isAssignableFrom(x)) {
            return Value.BLOB;
        } else if (Object[].class.isAssignableFrom(x)) {
            // this includes String[] and so on
            return Value.ARRAY;
        } else if (isGeometryClass(x)) {
            return Value.GEOMETRY;
        } else if (LocalDateTimeUtils.LOCAL_DATE == x) {
            return Value.DATE;
        } else if (LocalDateTimeUtils.LOCAL_TIME == x) {
            return Value.TIME;
        } else if (LocalDateTimeUtils.LOCAL_DATE_TIME == x) {
            return Value.TIMESTAMP;
        } else if (LocalDateTimeUtils.OFFSET_DATE_TIME == x || LocalDateTimeUtils.INSTANT == x) {
            return Value.TIMESTAMP_TZ;
        } else {
            if (JdbcUtils.customDataTypesHandler != null) {
                return JdbcUtils.customDataTypesHandler.getTypeIdFromClass(x);
            }
            return Value.JAVA_OBJECT;
        }
    }

    /**
     * Convert a Java object to a value.
     *
     * @param session the session
     * @param x the value
     * @param type the value type
     * @return the value
     */
    public static Value convertToValue(SessionInterface session, Object x,
            int type) {
        Value v = convertToValue1(session, x, type);
        if (session != null) {
            session.addTemporaryLob(v);
        }
        return v;
    }

    private static Value convertToValue1(SessionInterface session, Object x,
            int type) {
        if (x == null) {
            return ValueNull.INSTANCE;
        }
        if (type == Value.JAVA_OBJECT) {
            return ValueJavaObject.getNoCopy(x, null, session.getDataHandler());
        }
        if (x instanceof String) {
            return ValueString.get((String) x);
        } else if (x instanceof Value) {
            return (Value) x;
        } else if (x instanceof Long) {
            return ValueLong.get((Long) x);
        } else if (x instanceof Integer) {
            return ValueInt.get((Integer) x);
        } else if (x instanceof BigInteger) {
            return ValueDecimal.get(new BigDecimal((BigInteger) x));
        } else if (x instanceof BigDecimal) {
            return ValueDecimal.get((BigDecimal) x);
        } else if (x instanceof Boolean) {
            return ValueBoolean.get((Boolean) x);
        } else if (x instanceof Byte) {
            return ValueByte.get((Byte) x);
        } else if (x instanceof Short) {
            return ValueShort.get((Short) x);
        } else if (x instanceof Float) {
            return ValueFloat.get((Float) x);
        } else if (x instanceof Double) {
            return ValueDouble.get((Double) x);
        } else if (x instanceof byte[]) {
            return ValueBytes.get((byte[]) x);
        } else if (x instanceof Date) {
            return ValueDate.get((Date) x);
        } else if (x instanceof Time) {
            return ValueTime.get((Time) x);
        } else if (x instanceof Timestamp) {
            return ValueTimestamp.get((Timestamp) x);
        } else if (x instanceof java.util.Date) {
            return ValueTimestamp.fromMillis(((java.util.Date) x).getTime());
        } else if (x instanceof java.io.Reader) {
            Reader r = new BufferedReader((java.io.Reader) x);
            return session.getDataHandler().getLobStorage().
                    createClob(r, -1);
        } else if (x instanceof java.sql.Clob) {
            try {
                java.sql.Clob clob = (java.sql.Clob) x;
                Reader r = new BufferedReader(clob.getCharacterStream());
                return session.getDataHandler().getLobStorage().
                        createClob(r, clob.length());
            } catch (SQLException e) {
                throw DbException.convert(e);
            }
        } else if (x instanceof java.io.InputStream) {
            return session.getDataHandler().getLobStorage().
                    createBlob((java.io.InputStream) x, -1);
        } else if (x instanceof java.sql.Blob) {
            try {
                java.sql.Blob blob = (java.sql.Blob) x;
                return session.getDataHandler().getLobStorage().
                        createBlob(blob.getBinaryStream(), blob.length());
            } catch (SQLException e) {
                throw DbException.convert(e);
            }
        } else if (x instanceof java.sql.SQLXML) {
            try {
                java.sql.SQLXML clob = (java.sql.SQLXML) x;
                Reader r = new BufferedReader(clob.getCharacterStream());
                return session.getDataHandler().getLobStorage().
                        createClob(r, -1);
            } catch (SQLException e) {
                throw DbException.convert(e);
            }
        } else if (x instanceof java.sql.Array) {
            java.sql.Array array = (java.sql.Array) x;
            try {
                return convertToValue(session, array.getArray(), Value.ARRAY);
            } catch (SQLException e) {
                throw DbException.convert(e);
            }
        } else if (x instanceof ResultSet) {
            return ValueResultSet.get(session, (ResultSet) x, Integer.MAX_VALUE);
        } else if (x instanceof UUID) {
            return ValueUuid.get((UUID) x);
        }
        Class<?> clazz = x.getClass();
        if (x instanceof Object[]) {
            // (a.getClass().isArray());
            // (a.getClass().getComponentType().isPrimitive());
            Object[] o = (Object[]) x;
            int len = o.length;
            Value[] v = new Value[len];
            for (int i = 0; i < len; i++) {
                v[i] = convertToValue(session, o[i], type);
            }
            return ValueArray.get(clazz.getComponentType(), v);
        } else if (x instanceof Character) {
            return ValueStringFixed.get(((Character) x).toString());
        } else if (isGeometry(x)) {
            return ValueGeometry.getFromGeometry(x);
        } else if (clazz == LocalDateTimeUtils.LOCAL_DATE) {
            return LocalDateTimeUtils.localDateToDateValue(x);
        } else if (clazz == LocalDateTimeUtils.LOCAL_TIME) {
            return LocalDateTimeUtils.localTimeToTimeValue(x);
        } else if (clazz == LocalDateTimeUtils.LOCAL_DATE_TIME) {
            return LocalDateTimeUtils.localDateTimeToValue(x);
        } else if (clazz == LocalDateTimeUtils.INSTANT) {
            return LocalDateTimeUtils.instantToValue(x);
        } else if (clazz == LocalDateTimeUtils.OFFSET_DATE_TIME) {
            return LocalDateTimeUtils.offsetDateTimeToValue(x);
        } else if (x instanceof TimestampWithTimeZone) {
            return ValueTimestampTimeZone.get((TimestampWithTimeZone) x);
        } else if (x instanceof Interval) {
            Interval i = (Interval) x;
            return ValueInterval.from(i.getQualifier(), i.isNegative(), i.getLeading(), i.getRemaining());
        } else if (clazz == LocalDateTimeUtils.PERIOD) {
            return LocalDateTimeUtils.periodToValue(x);
        } else if (clazz == LocalDateTimeUtils.DURATION) {
            return LocalDateTimeUtils.durationToValue(x);
        } else {
            if (JdbcUtils.customDataTypesHandler != null) {
                return JdbcUtils.customDataTypesHandler.getValue(type, x,
                        session.getDataHandler());
            }
            return ValueJavaObject.getNoCopy(x, null, session.getDataHandler());
        }
    }


    /**
     * Check whether a given class matches the Geometry class.
     *
     * @param x the class
     * @return true if it is a Geometry class
     */
    public static boolean isGeometryClass(Class<?> x) {
        if (x == null || GEOMETRY_CLASS == null) {
            return false;
        }
        return GEOMETRY_CLASS.isAssignableFrom(x);
    }

    /**
     * Check whether a given object is a Geometry object.
     *
     * @param x the object
     * @return true if it is a Geometry object
     */
    public static boolean isGeometry(Object x) {
        if (x == null) {
            return false;
        }
        return isGeometryClass(x.getClass());
    }

    /**
     * Get a data type object from a type name.
     *
     * @param s the type name
     * @param mode database mode
     * @return the data type object
     */
    public static DataType getTypeByName(String s, Mode mode) {
        DataType result = mode.typeByNameMap.get(s);
        if (result == null) {
            result = TYPES_BY_NAME.get(s);
            if (result == null && JdbcUtils.customDataTypesHandler != null) {
                result = JdbcUtils.customDataTypesHandler.getDataTypeByName(s);
            }
        }
        return result;
    }

    /**
     * Check if the given value type is a date-time type (TIME, DATE, TIMESTAMP,
     * TIMESTAMP_TZ).
     *
     * @param type the value type
     * @return true if the value type is a date-time type
     */
    public static boolean isDateTimeType(int type) {
        switch (type) {
        case Value.TIME:
        case Value.DATE:
        case Value.TIMESTAMP:
        case Value.TIMESTAMP_TZ:
            return true;
        default:
            return false;
        }
    }

    /**
     * Check if the given value type is an interval type.
     *
     * @param type the value type
     * @return true if the value type is an interval type
     */
    public static boolean isIntervalType(int type) {
        return type >= Value.INTERVAL_YEAR && type <= Value.INTERVAL_MINUTE_TO_SECOND;
    }

    /**
     * Check if the given value type is a year-month interval type.
     *
     * @param type the value type
     * @return true if the value type is a year-month interval type
     */
    public static boolean isYearMonthIntervalType(int type) {
        return type == Value.INTERVAL_YEAR || type == Value.INTERVAL_MONTH || type == Value.INTERVAL_YEAR_TO_MONTH;
    }

    /**
     * Check if the given value type is a large object (BLOB or CLOB).
     *
     * @param type the value type
     * @return true if the value type is a lob type
     */
    public static boolean isLargeObject(int type) {
        return type == Value.BLOB || type == Value.CLOB;
    }

    /**
     * Check if the given value type is a numeric type.
     *
     * @param type the value type
     * @return true if the value type is a numeric type
     */
    public static boolean isNumericType(int type) {
        return type >= Value.BYTE && type <= Value.FLOAT;
    }

    /**
     * Check if the given value type is a String (VARCHAR,...).
     *
     * @param type the value type
     * @return true if the value type is a String type
     */
    public static boolean isStringType(int type) {
        return type == Value.STRING || type == Value.STRING_FIXED || type == Value.STRING_IGNORECASE;
    }

    /**
     * Check if the given type may have extended type information.
     *
     * @param type the value type
     * @return true if the value type may have extended type information
     */
    public static boolean isExtInfoType(int type) {
        return type == Value.GEOMETRY || type == Value.ENUM;
    }

    /**
     * Check if the given type has total ordering.
     *
     * @param type the value type
     * @return true if the value type has total ordering
     */
    public static boolean hasTotalOrdering(int type) {
        switch (type) {
        case Value.BOOLEAN:
        case Value.BYTE:
        case Value.SHORT:
        case Value.INT:
        case Value.LONG:
        // Negative zeroes and NaNs are normalized
        case Value.DOUBLE:
        case Value.FLOAT:
        case Value.TIME:
        case Value.DATE:
        case Value.TIMESTAMP:
        case Value.BYTES:
        // Serialized data is compared
        case Value.JAVA_OBJECT:
        case Value.UUID:
        // EWKB is used
        case Value.GEOMETRY:
        case Value.ENUM:
        case Value.INTERVAL_YEAR:
        case Value.INTERVAL_MONTH:
        case Value.INTERVAL_DAY:
        case Value.INTERVAL_HOUR:
        case Value.INTERVAL_MINUTE:
        case Value.INTERVAL_SECOND:
        case Value.INTERVAL_YEAR_TO_MONTH:
        case Value.INTERVAL_DAY_TO_HOUR:
        case Value.INTERVAL_DAY_TO_MINUTE:
        case Value.INTERVAL_DAY_TO_SECOND:
        case Value.INTERVAL_HOUR_TO_MINUTE:
        case Value.INTERVAL_HOUR_TO_SECOND:
        case Value.INTERVAL_MINUTE_TO_SECOND:
            return true;
        default:
            return false;
        }
    }

    /**
     * Check if the given value type supports the add operation.
     *
     * @param type the value type
     * @return true if add is supported
     */
    public static boolean supportsAdd(int type) {
        switch (type) {
        case Value.BYTE:
        case Value.DECIMAL:
        case Value.DOUBLE:
        case Value.FLOAT:
        case Value.INT:
        case Value.LONG:
        case Value.SHORT:
        case Value.INTERVAL_YEAR:
        case Value.INTERVAL_MONTH:
        case Value.INTERVAL_DAY:
        case Value.INTERVAL_HOUR:
        case Value.INTERVAL_MINUTE:
        case Value.INTERVAL_SECOND:
        case Value.INTERVAL_YEAR_TO_MONTH:
        case Value.INTERVAL_DAY_TO_HOUR:
        case Value.INTERVAL_DAY_TO_MINUTE:
        case Value.INTERVAL_DAY_TO_SECOND:
        case Value.INTERVAL_HOUR_TO_MINUTE:
        case Value.INTERVAL_HOUR_TO_SECOND:
        case Value.INTERVAL_MINUTE_TO_SECOND:
            return true;
        case Value.BOOLEAN:
        case Value.TIME:
        case Value.DATE:
        case Value.TIMESTAMP:
        case Value.TIMESTAMP_TZ:
        case Value.BYTES:
        case Value.UUID:
        case Value.STRING:
        case Value.STRING_IGNORECASE:
        case Value.STRING_FIXED:
        case Value.BLOB:
        case Value.CLOB:
        case Value.NULL:
        case Value.JAVA_OBJECT:
        case Value.UNKNOWN:
        case Value.ARRAY:
        case Value.RESULT_SET:
        case Value.GEOMETRY:
            return false;
        default:
            if (JdbcUtils.customDataTypesHandler != null) {
                return JdbcUtils.customDataTypesHandler.supportsAdd(type);
            }
            return false;
        }
    }

    /**
     * Get the data type that will not overflow when calling 'add' 2 billion
     * times.
     *
     * @param type the value type
     * @return the data type that supports adding
     */
    public static int getAddProofType(int type) {
        switch (type) {
        case Value.BYTE:
            return Value.LONG;
        case Value.FLOAT:
            return Value.DOUBLE;
        case Value.INT:
            return Value.LONG;
        case Value.LONG:
            return Value.DECIMAL;
        case Value.SHORT:
            return Value.LONG;
        case Value.BOOLEAN:
        case Value.DECIMAL:
        case Value.TIME:
        case Value.DATE:
        case Value.TIMESTAMP:
        case Value.TIMESTAMP_TZ:
        case Value.BYTES:
        case Value.UUID:
        case Value.STRING:
        case Value.STRING_IGNORECASE:
        case Value.STRING_FIXED:
        case Value.BLOB:
        case Value.CLOB:
        case Value.DOUBLE:
        case Value.NULL:
        case Value.JAVA_OBJECT:
        case Value.UNKNOWN:
        case Value.ARRAY:
        case Value.RESULT_SET:
        case Value.GEOMETRY:
        case Value.INTERVAL_YEAR:
        case Value.INTERVAL_MONTH:
        case Value.INTERVAL_DAY:
        case Value.INTERVAL_HOUR:
        case Value.INTERVAL_MINUTE:
        case Value.INTERVAL_SECOND:
        case Value.INTERVAL_YEAR_TO_MONTH:
        case Value.INTERVAL_DAY_TO_HOUR:
        case Value.INTERVAL_DAY_TO_MINUTE:
        case Value.INTERVAL_DAY_TO_SECOND:
        case Value.INTERVAL_HOUR_TO_MINUTE:
        case Value.INTERVAL_HOUR_TO_SECOND:
        case Value.INTERVAL_MINUTE_TO_SECOND:
            return type;
        default:
            if (JdbcUtils.customDataTypesHandler != null) {
                return JdbcUtils.customDataTypesHandler.getAddProofType(type);
            }
            return type;
        }
    }

    /**
     * Get the default value in the form of a Java object for the given Java
     * class.
     *
     * @param clazz the Java class
     * @return the default object
     */
    public static Object getDefaultForPrimitiveType(Class<?> clazz) {
        if (clazz == Boolean.TYPE) {
            return Boolean.FALSE;
        } else if (clazz == Byte.TYPE) {
            return (byte) 0;
        } else if (clazz == Character.TYPE) {
            return (char) 0;
        } else if (clazz == Short.TYPE) {
            return (short) 0;
        } else if (clazz == Integer.TYPE) {
            return 0;
        } else if (clazz == Long.TYPE) {
            return 0L;
        } else if (clazz == Float.TYPE) {
            return (float) 0;
        } else if (clazz == Double.TYPE) {
            return (double) 0;
        }
        throw DbException.throwInternalError(
                "primitive=" + clazz.toString());
    }

    /**
     * Convert a value to the specified class.
     *
     * @param conn the database connection
     * @param v the value
     * @param paramClass the target class
     * @return the converted object
     */
    public static Object convertTo(JdbcConnection conn, Value v,
            Class<?> paramClass) {
        if (paramClass == Blob.class) {
            return new JdbcBlob(conn, v, JdbcLob.State.WITH_VALUE, 0);
        } else if (paramClass == Clob.class) {
            return new JdbcClob(conn, v, JdbcLob.State.WITH_VALUE, 0);
        } else if (paramClass == Array.class) {
            return new JdbcArray(conn, v, 0);
        }
        switch (v.getValueType()) {
        case Value.JAVA_OBJECT: {
            Object o = SysProperties.serializeJavaObject ? JdbcUtils.deserialize(v.getBytes(),
                    conn.getSession().getDataHandler()) : v.getObject();
            if (paramClass.isAssignableFrom(o.getClass())) {
                return o;
            }
            break;
        }
        case Value.BOOLEAN:
        case Value.BYTE:
        case Value.SHORT:
        case Value.INT:
        case Value.LONG:
        case Value.DECIMAL:
        case Value.TIME:
        case Value.DATE:
        case Value.TIMESTAMP:
        case Value.TIMESTAMP_TZ:
        case Value.BYTES:
        case Value.UUID:
        case Value.STRING:
        case Value.STRING_IGNORECASE:
        case Value.STRING_FIXED:
        case Value.BLOB:
        case Value.CLOB:
        case Value.DOUBLE:
        case Value.FLOAT:
        case Value.NULL:
        case Value.UNKNOWN:
        case Value.ARRAY:
        case Value.RESULT_SET:
        case Value.GEOMETRY:
            break;
        default:
            if (JdbcUtils.customDataTypesHandler != null) {
                return JdbcUtils.customDataTypesHandler.getObject(v, paramClass);
            }
        }
        throw DbException.getUnsupportedException("converting to class " + paramClass.getName());
    }

}
