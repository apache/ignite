/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.message;

import java.text.MessageFormat;
import java.util.ArrayList;

import org.h2.engine.SysProperties;
import org.h2.expression.ParameterInterface;
import org.h2.util.StatementBuilder;
import org.h2.util.StringUtils;
import org.h2.value.Value;

/**
 * This class represents a trace module.
 */
public class Trace {

    /**
     * The trace module id for commands.
     */
    public static final int COMMAND = 0;

    /**
     * The trace module id for constraints.
     */
    public static final int CONSTRAINT = 1;

    /**
     * The trace module id for databases.
     */
    public static final int DATABASE = 2;

    /**
     * The trace module id for functions.
     */
    public static final int FUNCTION = 3;

    /**
     * The trace module id for file locks.
     */
    public static final int FILE_LOCK = 4;

    /**
     * The trace module id for indexes.
     */
    public static final int INDEX = 5;

    /**
     * The trace module id for the JDBC API.
     */
    public static final int JDBC = 6;

    /**
     * The trace module id for locks.
     */
    public static final int LOCK = 7;

    /**
     * The trace module id for schemas.
     */
    public static final int SCHEMA = 8;

    /**
     * The trace module id for sequences.
     */
    public static final int SEQUENCE = 9;

    /**
     * The trace module id for settings.
     */
    public static final int SETTING = 10;

    /**
     * The trace module id for tables.
     */
    public static final int TABLE = 11;

    /**
     * The trace module id for triggers.
     */
    public static final int TRIGGER = 12;

    /**
     * The trace module id for users.
     */
    public static final int USER = 13;

    /**
     * The trace module id for the page store.
     */
    public static final int PAGE_STORE = 14;

    /**
     * The trace module id for the JDBCX API
     */
    public static final int JDBCX = 15;

    /**
     * Module names by their ids as array indexes.
     */
    public static final String[] MODULE_NAMES = {
        "command",
        "constraint",
        "database",
        "function",
        "fileLock",
        "index",
        "jdbc",
        "lock",
        "schema",
        "sequence",
        "setting",
        "table",
        "trigger",
        "user",
        "pageStore",
        "JDBCX"
    };

    private final TraceWriter traceWriter;
    private final String module;
    private final String lineSeparator;
    private int traceLevel = TraceSystem.PARENT;

    Trace(TraceWriter traceWriter, int moduleId) {
        this(traceWriter, MODULE_NAMES[moduleId]);
    }

    Trace(TraceWriter traceWriter, String module) {
        this.traceWriter = traceWriter;
        this.module = module;
        this.lineSeparator = SysProperties.LINE_SEPARATOR;
    }

    /**
     * Set the trace level of this component. This setting overrides the parent
     * trace level.
     *
     * @param level the new level
     */
    public void setLevel(int level) {
        this.traceLevel = level;
    }

    private boolean isEnabled(int level) {
        if (this.traceLevel == TraceSystem.PARENT) {
            return traceWriter.isEnabled(level);
        }
        return level <= this.traceLevel;
    }

    /**
     * Check if the trace level is equal or higher than INFO.
     *
     * @return true if it is
     */
    public boolean isInfoEnabled() {
        return isEnabled(TraceSystem.INFO);
    }

    /**
     * Check if the trace level is equal or higher than DEBUG.
     *
     * @return true if it is
     */
    public boolean isDebugEnabled() {
        return isEnabled(TraceSystem.DEBUG);
    }

    /**
     * Write a message with trace level ERROR to the trace system.
     *
     * @param t the exception
     * @param s the message
     */
    public void error(Throwable t, String s) {
        if (isEnabled(TraceSystem.ERROR)) {
            traceWriter.write(TraceSystem.ERROR, module, s, t);
        }
    }

    /**
     * Write a message with trace level ERROR to the trace system.
     *
     * @param t the exception
     * @param s the message
     * @param params the parameters
     */
    public void error(Throwable t, String s, Object... params) {
        if (isEnabled(TraceSystem.ERROR)) {
            s = MessageFormat.format(s, params);
            traceWriter.write(TraceSystem.ERROR, module, s, t);
        }
    }

    /**
     * Write a message with trace level INFO to the trace system.
     *
     * @param s the message
     */
    public void info(String s) {
        if (isEnabled(TraceSystem.INFO)) {
            traceWriter.write(TraceSystem.INFO, module, s, null);
        }
    }

    /**
     * Write a message with trace level INFO to the trace system.
     *
     * @param s the message
     * @param params the parameters
     */
    public void info(String s, Object... params) {
        if (isEnabled(TraceSystem.INFO)) {
            s = MessageFormat.format(s, params);
            traceWriter.write(TraceSystem.INFO, module, s, null);
        }
    }

    /**
     * Write a message with trace level INFO to the trace system.
     *
     * @param t the exception
     * @param s the message
     */
    void info(Throwable t, String s) {
        if (isEnabled(TraceSystem.INFO)) {
            traceWriter.write(TraceSystem.INFO, module, s, t);
        }
    }

    /**
     * Format the parameter list.
     *
     * @param parameters the parameter list
     * @return the formatted text
     */
    public static String formatParams(
            ArrayList<? extends ParameterInterface> parameters) {
        if (parameters.isEmpty()) {
            return "";
        }
        StatementBuilder buff = new StatementBuilder();
        int i = 0;
        boolean params = false;
        for (ParameterInterface p : parameters) {
            if (p.isValueSet()) {
                if (!params) {
                    buff.append(" {");
                    params = true;
                }
                buff.appendExceptFirst(", ");
                Value v = p.getParamValue();
                buff.append(++i).append(": ").append(v.getTraceSQL());
            }
        }
        if (params) {
            buff.append('}');
        }
        return buff.toString();
    }

    /**
     * Write a SQL statement with trace level INFO to the trace system.
     *
     * @param sql the SQL statement
     * @param params the parameters used, in the for {1:...}
     * @param count the update count
     * @param time the time it took to run the statement in ms
     */
    public void infoSQL(String sql, String params, int count, long time) {
        if (!isEnabled(TraceSystem.INFO)) {
            return;
        }
        StringBuilder buff = new StringBuilder(sql.length() + params.length() + 20);
        buff.append(lineSeparator).append("/*SQL");
        boolean space = false;
        if (params.length() > 0) {
            // This looks like a bug, but it is intentional:
            // If there are no parameters, the SQL statement is
            // the rest of the line. If there are parameters, they
            // are appended at the end of the line. Knowing the size
            // of the statement simplifies separating the SQL statement
            // from the parameters (no need to parse).
            space = true;
            buff.append(" l:").append(sql.length());
        }
        if (count > 0) {
            space = true;
            buff.append(" #:").append(count);
        }
        if (time > 0) {
            space = true;
            buff.append(" t:").append(time);
        }
        if (!space) {
            buff.append(' ');
        }
        buff.append("*/").
            append(StringUtils.javaEncode(sql)).
            append(StringUtils.javaEncode(params)).
            append(';');
        sql = buff.toString();
        traceWriter.write(TraceSystem.INFO, module, sql, null);
    }

    /**
     * Write a message with trace level DEBUG to the trace system.
     *
     * @param s the message
     * @param params the parameters
     */
    public void debug(String s, Object... params) {
        if (isEnabled(TraceSystem.DEBUG)) {
            s = MessageFormat.format(s, params);
            traceWriter.write(TraceSystem.DEBUG, module, s, null);
        }
    }

    /**
     * Write a message with trace level DEBUG to the trace system.
     *
     * @param s the message
     */
    public void debug(String s) {
        if (isEnabled(TraceSystem.DEBUG)) {
            traceWriter.write(TraceSystem.DEBUG, module, s, null);
        }
    }

    /**
     * Write a message with trace level DEBUG to the trace system.
     * @param t the exception
     * @param s the message
     */
    public void debug(Throwable t, String s) {
        if (isEnabled(TraceSystem.DEBUG)) {
            traceWriter.write(TraceSystem.DEBUG, module, s, t);
        }
    }


    /**
     * Write Java source code with trace level INFO to the trace system.
     *
     * @param java the source code
     */
    public void infoCode(String java) {
        if (isEnabled(TraceSystem.INFO)) {
            traceWriter.write(TraceSystem.INFO, module, lineSeparator +
                    "/**/" + java, null);
        }
    }

    /**
     * Write Java source code with trace level DEBUG to the trace system.
     *
     * @param java the source code
     */
    void debugCode(String java) {
        if (isEnabled(TraceSystem.DEBUG)) {
            traceWriter.write(TraceSystem.DEBUG, module, lineSeparator +
                    "/**/" + java, null);
        }
    }

}
