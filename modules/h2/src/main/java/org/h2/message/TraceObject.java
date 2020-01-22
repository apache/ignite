/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.message;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicIntegerArray;

import org.h2.util.StringUtils;

/**
 * The base class for objects that can print trace information about themselves.
 */
public class TraceObject {

    /**
     * The trace type id  for callable statements.
     */
    protected static final int CALLABLE_STATEMENT = 0;

    /**
     * The trace type id  for connections.
     */
    protected static final int CONNECTION = 1;

    /**
     * The trace type id  for database meta data objects.
     */
    protected static final int DATABASE_META_DATA = 2;

    /**
     * The trace type id  for prepared statements.
     */
    protected static final int PREPARED_STATEMENT = 3;

    /**
     * The trace type id  for result sets.
     */
    protected static final int RESULT_SET = 4;

    /**
     * The trace type id  for result set meta data objects.
     */
    protected static final int RESULT_SET_META_DATA = 5;

    /**
     * The trace type id  for savepoint objects.
     */
    protected static final int SAVEPOINT = 6;

    /**
     * The trace type id  for statements.
     */
    protected static final int STATEMENT = 8;

    /**
     * The trace type id  for blobs.
     */
    protected static final int BLOB = 9;

    /**
     * The trace type id  for clobs.
     */
    protected static final int CLOB = 10;

    /**
     * The trace type id  for parameter meta data objects.
     */
    protected static final int PARAMETER_META_DATA = 11;

    /**
     * The trace type id  for data sources.
     */
    protected static final int DATA_SOURCE = 12;

    /**
     * The trace type id  for XA data sources.
     */
    protected static final int XA_DATA_SOURCE = 13;

    /**
     * The trace type id  for transaction ids.
     */
    protected static final int XID = 15;

    /**
     * The trace type id  for array objects.
     */
    protected static final int ARRAY = 16;

    private static final int LAST = ARRAY + 1;
    private static final AtomicIntegerArray ID = new AtomicIntegerArray(LAST);

    private static final String[] PREFIX = { "call", "conn", "dbMeta", "prep",
            "rs", "rsMeta", "sp", "ex", "stat", "blob", "clob", "pMeta", "ds",
            "xads", "xares", "xid", "ar" };

    /**
     * The trace module used by this object.
     */
    protected Trace trace;

    private int traceType;
    private int id;

    /**
     * Set the options to use when writing trace message.
     *
     * @param trace the trace object
     * @param type the trace object type
     * @param id the trace object id
     */
    protected void setTrace(Trace trace, int type, int id) {
        this.trace = trace;
        this.traceType = type;
        this.id = id;
    }

    /**
     * INTERNAL
     */
    public int getTraceId() {
        return id;
    }

    /**
     * INTERNAL
     */
    public String getTraceObjectName() {
        return PREFIX[traceType] + id;
    }

    /**
     * Get the next trace object id for this object type.
     *
     * @param type the object type
     * @return the new trace object id
     */
    protected static int getNextId(int type) {
        return ID.getAndIncrement(type);
    }

    /**
     * Check if the debug trace level is enabled.
     *
     * @return true if it is
     */
    protected boolean isDebugEnabled() {
        return trace.isDebugEnabled();
    }

    /**
     * Check if info trace level is enabled.
     *
     * @return true if it is
     */
    protected boolean isInfoEnabled() {
        return trace.isInfoEnabled();
    }

    /**
     * Write trace information as an assignment in the form
     * className prefixId = objectName.value.
     *
     * @param className the class name of the result
     * @param newType the prefix type
     * @param newId the trace object id of the created object
     * @param value the value to assign this new object to
     */
    protected void debugCodeAssign(String className, int newType, int newId,
            String value) {
        if (trace.isDebugEnabled()) {
            trace.debugCode(className + " " + PREFIX[newType] +
                    newId + " = " + getTraceObjectName() + "." + value + ";");
        }
    }

    /**
     * Write trace information as a method call in the form
     * objectName.methodName().
     *
     * @param methodName the method name
     */
    protected void debugCodeCall(String methodName) {
        if (trace.isDebugEnabled()) {
            trace.debugCode(getTraceObjectName() + "." + methodName + "();");
        }
    }

    /**
     * Write trace information as a method call in the form
     * objectName.methodName(param) where the parameter is formatted as a long
     * value.
     *
     * @param methodName the method name
     * @param param one single long parameter
     */
    protected void debugCodeCall(String methodName, long param) {
        if (trace.isDebugEnabled()) {
            trace.debugCode(getTraceObjectName() + "." +
                    methodName + "(" + param + ");");
        }
    }

    /**
     * Write trace information as a method call in the form
     * objectName.methodName(param) where the parameter is formatted as a Java
     * string.
     *
     * @param methodName the method name
     * @param param one single string parameter
     */
    protected void debugCodeCall(String methodName, String param) {
        if (trace.isDebugEnabled()) {
            trace.debugCode(getTraceObjectName() + "." +
                    methodName + "(" + quote(param) + ");");
        }
    }

    /**
     * Write trace information in the form objectName.text.
     *
     * @param text the trace text
     */
    protected void debugCode(String text) {
        if (trace.isDebugEnabled()) {
            trace.debugCode(getTraceObjectName() + "." + text);
        }
    }

    /**
     * Format a string as a Java string literal.
     *
     * @param s the string to convert
     * @return the Java string literal
     */
    protected static String quote(String s) {
        return StringUtils.quoteJavaString(s);
    }

    /**
     * Format a time to the Java source code that represents this object.
     *
     * @param x the time to convert
     * @return the Java source code
     */
    protected static String quoteTime(java.sql.Time x) {
        if (x == null) {
            return "null";
        }
        return "Time.valueOf(\"" + x.toString() + "\")";
    }

    /**
     * Format a timestamp to the Java source code that represents this object.
     *
     * @param x the timestamp to convert
     * @return the Java source code
     */
    protected static String quoteTimestamp(java.sql.Timestamp x) {
        if (x == null) {
            return "null";
        }
        return "Timestamp.valueOf(\"" + x.toString() + "\")";
    }

    /**
     * Format a date to the Java source code that represents this object.
     *
     * @param x the date to convert
     * @return the Java source code
     */
    protected static String quoteDate(java.sql.Date x) {
        if (x == null) {
            return "null";
        }
        return "Date.valueOf(\"" + x.toString() + "\")";
    }

    /**
     * Format a big decimal to the Java source code that represents this object.
     *
     * @param x the big decimal to convert
     * @return the Java source code
     */
    protected static String quoteBigDecimal(BigDecimal x) {
        if (x == null) {
            return "null";
        }
        return "new BigDecimal(\"" + x.toString() + "\")";
    }

    /**
     * Format a byte array to the Java source code that represents this object.
     *
     * @param x the byte array to convert
     * @return the Java source code
     */
    protected static String quoteBytes(byte[] x) {
        if (x == null) {
            return "null";
        }
        return "org.h2.util.StringUtils.convertHexToBytes(\"" +
                StringUtils.convertBytesToHex(x) + "\")";
    }

    /**
     * Format a string array to the Java source code that represents this
     * object.
     *
     * @param s the string array to convert
     * @return the Java source code
     */
    protected static String quoteArray(String[] s) {
        return StringUtils.quoteJavaStringArray(s);
    }

    /**
     * Format an int array to the Java source code that represents this object.
     *
     * @param s the int array to convert
     * @return the Java source code
     */
    protected static String quoteIntArray(int[] s) {
        return StringUtils.quoteJavaIntArray(s);
    }

    /**
     * Format a map to the Java source code that represents this object.
     *
     * @param map the map to convert
     * @return the Java source code
     */
    protected static String quoteMap(Map<String, Class<?>> map) {
        if (map == null) {
            return "null";
        }
        if (map.size() == 0) {
            return "new Map()";
        }
        return "new Map() /* " + map.toString() + " */";
    }

    /**
     * Log an exception and convert it to a SQL exception if required.
     *
     * @param ex the exception
     * @return the SQL exception object
     */
    protected SQLException logAndConvert(Throwable ex) {
        SQLException e = null;
        try {
            e = DbException.toSQLException(ex);
            if (trace == null) {
                DbException.traceThrowable(e);
            } else {
                int errorCode = e.getErrorCode();
                if (errorCode >= 23000 && errorCode < 24000) {
                    trace.info(e, "exception");
                } else {
                    trace.error(e, "exception");
                }
            }
        } catch(Throwable ignore) {
            if (e == null) {
                e = new SQLException("", "HY000", ex);
            }
            e.addSuppressed(ignore);
        }
        return e;
    }

    /**
     * Get a SQL exception meaning this feature is not supported.
     *
     * @param message the message
     * @return the SQL exception
     */
    protected SQLException unsupported(String message) {
        try {
            throw DbException.getUnsupportedException(message);
        } catch (Exception e) {
            return logAndConvert(e);
        }
    }

}
