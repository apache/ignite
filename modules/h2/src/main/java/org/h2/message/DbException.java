/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.message;

import static org.h2.api.ErrorCode.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.Locale;
import java.util.Map.Entry;
import java.util.Properties;

import org.h2.engine.Constants;
import org.h2.jdbc.JdbcException;
import org.h2.jdbc.JdbcSQLDataException;
import org.h2.jdbc.JdbcSQLException;
import org.h2.jdbc.JdbcSQLFeatureNotSupportedException;
import org.h2.jdbc.JdbcSQLIntegrityConstraintViolationException;
import org.h2.jdbc.JdbcSQLInvalidAuthorizationSpecException;
import org.h2.jdbc.JdbcSQLNonTransientConnectionException;
import org.h2.jdbc.JdbcSQLNonTransientException;
import org.h2.jdbc.JdbcSQLSyntaxErrorException;
import org.h2.jdbc.JdbcSQLTimeoutException;
import org.h2.jdbc.JdbcSQLTransactionRollbackException;
import org.h2.jdbc.JdbcSQLTransientException;
import org.h2.util.SortedProperties;
import org.h2.util.StringUtils;
import org.h2.util.Utils;

/**
 * This exception wraps a checked exception.
 * It is used in methods where checked exceptions are not supported,
 * for example in a Comparator.
 */
public class DbException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * If the SQL statement contains this text, then it is never added to the
     * SQL exception. Hiding the SQL statement may be important if it contains a
     * passwords, such as a CREATE LINKED TABLE statement.
     */
    public static final String HIDE_SQL = "--hide--";

    private static final Properties MESSAGES = new Properties();

    /**
     * Thrown when OOME exception happens on handle error
     * inside {@link #convert(java.lang.Throwable)}.
     */
    public static final SQLException SQL_OOME =
            new SQLException("OutOfMemoryError", "HY000", OUT_OF_MEMORY, new OutOfMemoryError());
    private static final DbException OOME = new DbException(SQL_OOME);

    private Object source;

    static {
        try {
            byte[] messages = Utils.getResource(
                    "/org/h2/res/_messages_en.prop");
            if (messages != null) {
                MESSAGES.load(new ByteArrayInputStream(messages));
            }
            String language = Locale.getDefault().getLanguage();
            if (!"en".equals(language)) {
                byte[] translations = Utils.getResource(
                        "/org/h2/res/_messages_" + language + ".prop");
                // message: translated message + english
                // (otherwise certain applications don't work)
                if (translations != null) {
                    Properties p = SortedProperties.fromLines(
                            new String(translations, StandardCharsets.UTF_8));
                    for (Entry<Object, Object> e : p.entrySet()) {
                        String key = (String) e.getKey();
                        String translation = (String) e.getValue();
                        if (translation != null && !translation.startsWith("#")) {
                            String original = MESSAGES.getProperty(key);
                            String message = translation + "\n" + original;
                            MESSAGES.put(key, message);
                        }
                    }
                }
            }
        } catch (OutOfMemoryError | IOException e) {
            DbException.traceThrowable(e);
        }
    }

    private DbException(SQLException e) {
        super(e.getMessage(), e);
    }

    private static String translate(String key, String... params) {
        String message = null;
        if (MESSAGES != null) {
            // Tomcat sets final static fields to null sometimes
            message = MESSAGES.getProperty(key);
        }
        if (message == null) {
            message = "(Message " + key + " not found)";
        }
        if (params != null) {
            for (int i = 0; i < params.length; i++) {
                String s = params[i];
                if (s != null && s.length() > 0) {
                    params[i] = StringUtils.quoteIdentifier(s);
                }
            }
            message = MessageFormat.format(message, (Object[]) params);
        }
        return message;
    }

    /**
     * Get the SQLException object.
     *
     * @return the exception
     */
    public SQLException getSQLException() {
        return (SQLException) getCause();
    }

    /**
     * Get the error code.
     *
     * @return the error code
     */
    public int getErrorCode() {
        return getSQLException().getErrorCode();
    }

    /**
     * Set the SQL statement of the given exception.
     * This method may create a new object.
     *
     * @param sql the SQL statement
     * @return the exception
     */
    public DbException addSQL(String sql) {
        SQLException e = getSQLException();
        if (e instanceof JdbcException) {
            JdbcException j = (JdbcException) e;
            if (j.getSQL() == null) {
                j.setSQL(filterSQL(sql));
            }
            return this;
        }
        e = getJdbcSQLException(e.getMessage(), sql, e.getSQLState(), e.getErrorCode(), e, null);
        return new DbException(e);
    }

    /**
     * Create a database exception for a specific error code.
     *
     * @param errorCode the error code
     * @return the exception
     */
    public static DbException get(int errorCode) {
        return get(errorCode, (String) null);
    }

    /**
     * Create a database exception for a specific error code.
     *
     * @param errorCode the error code
     * @param p1 the first parameter of the message
     * @return the exception
     */
    public static DbException get(int errorCode, String p1) {
        return get(errorCode, new String[] { p1 });
    }

    /**
     * Create a database exception for a specific error code.
     *
     * @param errorCode the error code
     * @param cause the cause of the exception
     * @param params the list of parameters of the message
     * @return the exception
     */
    public static DbException get(int errorCode, Throwable cause,
            String... params) {
        return new DbException(getJdbcSQLException(errorCode, cause, params));
    }

    /**
     * Create a database exception for a specific error code.
     *
     * @param errorCode the error code
     * @param params the list of parameters of the message
     * @return the exception
     */
    public static DbException get(int errorCode, String... params) {
        return new DbException(getJdbcSQLException(errorCode, null, params));
    }

    /**
     * Create a database exception for an arbitrary SQLState.
     *
     * @param sqlstate the state to use
     * @param message the message to use
     * @return the exception
     */
    public static DbException fromUser(String sqlstate, String message) {
        // do not translate as sqlstate is arbitrary : avoid "message not found"
        return new DbException(getJdbcSQLException(message, null, sqlstate, 0, null, null));
    }

    /**
     * Create a syntax error exception.
     *
     * @param sql the SQL statement
     * @param index the position of the error in the SQL statement
     * @return the exception
     */
    public static DbException getSyntaxError(String sql, int index) {
        sql = StringUtils.addAsterisk(sql, index);
        return get(SYNTAX_ERROR_1, sql);
    }

    /**
     * Create a syntax error exception.
     *
     * @param sql the SQL statement
     * @param index the position of the error in the SQL statement
     * @param message the message
     * @return the exception
     */
    public static DbException getSyntaxError(String sql, int index,
            String message) {
        sql = StringUtils.addAsterisk(sql, index);
        return new DbException(getJdbcSQLException(SYNTAX_ERROR_2, null, sql, message));
    }

    /**
     * Create a syntax error exception for a specific error code.
     *
     * @param errorCode the error code
     * @param sql the SQL statement
     * @param index the position of the error in the SQL statement
     * @param params the list of parameters of the message
     * @return the exception
     */
    public static DbException getSyntaxError(int errorCode, String sql, int index, String... params) {
        sql = StringUtils.addAsterisk(sql, index);
        String sqlstate = getState(errorCode);
        String message = translate(sqlstate, params);
        return new DbException(getJdbcSQLException(message, sql, sqlstate, errorCode, null, null));
    }

    /**
     * Gets a SQL exception meaning this feature is not supported.
     *
     * @param message what exactly is not supported
     * @return the exception
     */
    public static DbException getUnsupportedException(String message) {
        return get(FEATURE_NOT_SUPPORTED_1, message);
    }

    /**
     * Gets a SQL exception meaning this value is invalid.
     *
     * @param param the name of the parameter
     * @param value the value passed
     * @return the IllegalArgumentException object
     */
    public static DbException getInvalidValueException(String param, Object value) {
        return get(INVALID_VALUE_2, value == null ? "null" : value.toString(), param);
    }

    /**
     * Throw an internal error. This method seems to return an exception object,
     * so that it can be used instead of 'return', but in fact it always throws
     * the exception.
     *
     * @param s the message
     * @return the RuntimeException object
     * @throws RuntimeException the exception
     */
    public static RuntimeException throwInternalError(String s) {
        RuntimeException e = new RuntimeException(s);
        DbException.traceThrowable(e);
        throw e;
    }

    /**
     * Throw an internal error. This method seems to return an exception object,
     * so that it can be used instead of 'return', but in fact it always throws
     * the exception.
     *
     * @return the RuntimeException object
     */
    public static RuntimeException throwInternalError() {
        return throwInternalError("Unexpected code path");
    }

    /**
     * Convert an exception to a SQL exception using the default mapping.
     *
     * @param e the root cause
     * @return the SQL exception object
     */
    public static SQLException toSQLException(Throwable e) {
        if (e instanceof SQLException) {
            return (SQLException) e;
        }
        return convert(e).getSQLException();
    }

    /**
     * Convert a throwable to an SQL exception using the default mapping. All
     * errors except the following are re-thrown: StackOverflowError,
     * LinkageError.
     *
     * @param e the root cause
     * @return the exception object
     */
    public static DbException convert(Throwable e) {
        try {
            if (e instanceof DbException) {
                return (DbException) e;
            } else if (e instanceof SQLException) {
                return new DbException((SQLException) e);
            } else if (e instanceof InvocationTargetException) {
                return convertInvocation((InvocationTargetException) e, null);
            } else if (e instanceof IOException) {
                return get(IO_EXCEPTION_1, e, e.toString());
            } else if (e instanceof OutOfMemoryError) {
                return get(OUT_OF_MEMORY, e);
            } else if (e instanceof StackOverflowError || e instanceof LinkageError) {
                return get(GENERAL_ERROR_1, e, e.toString());
            } else if (e instanceof Error) {
                throw (Error) e;
            }
            return get(GENERAL_ERROR_1, e, e.toString());
        } catch (Throwable ex) {
            try {
                DbException dbException = new DbException(
                        new SQLException("GeneralError", "HY000", GENERAL_ERROR_1, e));
                dbException.addSuppressed(ex);
                return dbException;
            } catch (OutOfMemoryError ignore) {
                return OOME;
            }
        }
    }

    /**
     * Convert an InvocationTarget exception to a database exception.
     *
     * @param te the root cause
     * @param message the added message or null
     * @return the database exception object
     */
    public static DbException convertInvocation(InvocationTargetException te,
            String message) {
        Throwable t = te.getTargetException();
        if (t instanceof SQLException || t instanceof DbException) {
            return convert(t);
        }
        message = message == null ? t.getMessage() : message + ": " + t.getMessage();
        return get(EXCEPTION_IN_FUNCTION_1, t, message);
    }

    /**
     * Convert an IO exception to a database exception.
     *
     * @param e the root cause
     * @param message the message or null
     * @return the database exception object
     */
    public static DbException convertIOException(IOException e, String message) {
        if (message == null) {
            Throwable t = e.getCause();
            if (t instanceof DbException) {
                return (DbException) t;
            }
            return get(IO_EXCEPTION_1, e, e.toString());
        }
        return get(IO_EXCEPTION_2, e, e.toString(), message);
    }

    /**
     * Gets the SQL exception object for a specific error code.
     *
     * @param errorCode the error code
     * @return the SQLException object
     */
    public static SQLException getJdbcSQLException(int errorCode) {
        return getJdbcSQLException(errorCode, (Throwable)null);
    }

    /**
     * Gets the SQL exception object for a specific error code.
     *
     * @param errorCode the error code
     * @param p1 the first parameter of the message
     * @return the SQLException object
     */
    public static SQLException getJdbcSQLException(int errorCode, String p1) {
        return getJdbcSQLException(errorCode, null, p1);
    }

    /**
     * Gets the SQL exception object for a specific error code.
     *
     * @param errorCode the error code
     * @param cause the cause of the exception
     * @param params the list of parameters of the message
     * @return the SQLException object
     */
    public static SQLException getJdbcSQLException(int errorCode, Throwable cause, String... params) {
        String sqlstate = getState(errorCode);
        String message = translate(sqlstate, params);
        return getJdbcSQLException(message, null, sqlstate, errorCode, cause, null);
    }

    /**
     * Creates a SQLException.
     *
     * @param message the reason
     * @param sql the SQL statement
     * @param state the SQL state
     * @param errorCode the error code
     * @param cause the exception that was the reason for this exception
     * @param stackTrace the stack trace
     * @return the SQLException object
     */
    public static SQLException getJdbcSQLException(String message, String sql, String state, int errorCode,
            Throwable cause, String stackTrace) {
        sql = filterSQL(sql);
        // Use SQLState class value to detect type
        switch (errorCode / 1_000) {
        case 2:
            return new JdbcSQLNonTransientException(message, sql, state, errorCode, cause, stackTrace);
        case 7:
        case 21:
        case 42:
            return new JdbcSQLSyntaxErrorException(message, sql, state, errorCode, cause, stackTrace);
        case 8:
            return new JdbcSQLNonTransientConnectionException(message, sql, state, errorCode, cause, stackTrace);
        case 22:
            return new JdbcSQLDataException(message, sql, state, errorCode, cause, stackTrace);
        case 23:
            return new JdbcSQLIntegrityConstraintViolationException(message, sql, state, errorCode, cause, stackTrace);
        case 28:
            return new JdbcSQLInvalidAuthorizationSpecException(message, sql, state, errorCode, cause, stackTrace);
        case 40:
            return new JdbcSQLTransactionRollbackException(message, sql, state, errorCode, cause, stackTrace);
        }
        // Check error code
        switch (errorCode){
        case GENERAL_ERROR_1:
        case UNKNOWN_DATA_TYPE_1:
        case METHOD_NOT_ALLOWED_FOR_QUERY:
        case METHOD_ONLY_ALLOWED_FOR_QUERY:
        case SEQUENCE_EXHAUSTED:
        case OBJECT_CLOSED:
        case CANNOT_DROP_CURRENT_USER:
        case UNSUPPORTED_SETTING_COMBINATION:
        case FILE_RENAME_FAILED_2:
        case FILE_DELETE_FAILED_1:
        case IO_EXCEPTION_1:
        case NOT_ON_UPDATABLE_ROW:
        case IO_EXCEPTION_2:
        case TRACE_FILE_ERROR_2:
        case ADMIN_RIGHTS_REQUIRED:
        case ERROR_EXECUTING_TRIGGER_3:
        case COMMIT_ROLLBACK_NOT_ALLOWED:
        case FILE_CREATION_FAILED_1:
        case SAVEPOINT_IS_INVALID_1:
        case SAVEPOINT_IS_UNNAMED:
        case SAVEPOINT_IS_NAMED:
        case NOT_ENOUGH_RIGHTS_FOR_1:
        case DATABASE_IS_READ_ONLY:
        case WRONG_XID_FORMAT_1:
        case UNSUPPORTED_COMPRESSION_OPTIONS_1:
        case UNSUPPORTED_COMPRESSION_ALGORITHM_1:
        case COMPRESSION_ERROR:
        case EXCEPTION_IN_FUNCTION_1:
        case ERROR_ACCESSING_LINKED_TABLE_2:
        case FILE_NOT_FOUND_1:
        case INVALID_CLASS_2:
        case DATABASE_IS_NOT_PERSISTENT:
        case RESULT_SET_NOT_UPDATABLE:
        case RESULT_SET_NOT_SCROLLABLE:
        case METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT:
        case ACCESS_DENIED_TO_CLASS_1:
        case RESULT_SET_READONLY:
            return new JdbcSQLNonTransientException(message, sql, state, errorCode, cause, stackTrace);
        case FEATURE_NOT_SUPPORTED_1:
            return new JdbcSQLFeatureNotSupportedException(message, sql, state, errorCode, cause, stackTrace);
        case LOCK_TIMEOUT_1:
        case STATEMENT_WAS_CANCELED:
        case LOB_CLOSED_ON_TIMEOUT_1:
            return new JdbcSQLTimeoutException(message, sql, state, errorCode, cause, stackTrace);
        case FUNCTION_MUST_RETURN_RESULT_SET_1:
        case TRIGGER_SELECT_AND_ROW_BASED_NOT_SUPPORTED:
        case SUM_OR_AVG_ON_WRONG_DATATYPE_1:
        case MUST_GROUP_BY_COLUMN_1:
        case SECOND_PRIMARY_KEY:
        case FUNCTION_NOT_FOUND_1:
        case COLUMN_MUST_NOT_BE_NULLABLE_1:
        case USER_NOT_FOUND_1:
        case USER_ALREADY_EXISTS_1:
        case SEQUENCE_ALREADY_EXISTS_1:
        case SEQUENCE_NOT_FOUND_1:
        case VIEW_NOT_FOUND_1:
        case VIEW_ALREADY_EXISTS_1:
        case TRIGGER_ALREADY_EXISTS_1:
        case TRIGGER_NOT_FOUND_1:
        case ERROR_CREATING_TRIGGER_OBJECT_3:
        case CONSTRAINT_ALREADY_EXISTS_1:
        case INVALID_VALUE_SCALE_PRECISION:
        case SUBQUERY_IS_NOT_SINGLE_COLUMN:
        case INVALID_USE_OF_AGGREGATE_FUNCTION_1:
        case CONSTRAINT_NOT_FOUND_1:
        case AMBIGUOUS_COLUMN_NAME_1:
        case ORDER_BY_NOT_IN_RESULT:
        case ROLE_ALREADY_EXISTS_1:
        case ROLE_NOT_FOUND_1:
        case USER_OR_ROLE_NOT_FOUND_1:
        case ROLES_AND_RIGHT_CANNOT_BE_MIXED:
        case METHODS_MUST_HAVE_DIFFERENT_PARAMETER_COUNTS_2:
        case ROLE_ALREADY_GRANTED_1:
        case COLUMN_IS_PART_OF_INDEX_1:
        case FUNCTION_ALIAS_ALREADY_EXISTS_1:
        case FUNCTION_ALIAS_NOT_FOUND_1:
        case SCHEMA_ALREADY_EXISTS_1:
        case SCHEMA_NOT_FOUND_1:
        case SCHEMA_NAME_MUST_MATCH:
        case COLUMN_CONTAINS_NULL_VALUES_1:
        case SEQUENCE_BELONGS_TO_A_TABLE_1:
        case COLUMN_IS_REFERENCED_1:
        case CANNOT_DROP_LAST_COLUMN:
        case INDEX_BELONGS_TO_CONSTRAINT_2:
        case CLASS_NOT_FOUND_1:
        case METHOD_NOT_FOUND_1:
        case COLLATION_CHANGE_WITH_DATA_TABLE_1:
        case SCHEMA_CAN_NOT_BE_DROPPED_1:
        case ROLE_CAN_NOT_BE_DROPPED_1:
        case CANNOT_TRUNCATE_1:
        case CANNOT_DROP_2:
        case VIEW_IS_INVALID_2:
        case COMPARING_ARRAY_TO_SCALAR:
        case CONSTANT_ALREADY_EXISTS_1:
        case CONSTANT_NOT_FOUND_1:
        case LITERALS_ARE_NOT_ALLOWED:
        case CANNOT_DROP_TABLE_1:
        case DOMAIN_ALREADY_EXISTS_1:
        case DOMAIN_NOT_FOUND_1:
        case WITH_TIES_WITHOUT_ORDER_BY:
        case CANNOT_MIX_INDEXED_AND_UNINDEXED_PARAMS:
        case TRANSACTION_NOT_FOUND_1:
        case AGGREGATE_NOT_FOUND_1:
        case WINDOW_NOT_FOUND_1:
        case CAN_ONLY_ASSIGN_TO_VARIABLE_1:
        case PUBLIC_STATIC_JAVA_METHOD_NOT_FOUND_1:
        case JAVA_OBJECT_SERIALIZER_CHANGE_WITH_DATA_TABLE:
        case FOR_UPDATE_IS_NOT_ALLOWED_IN_DISTINCT_OR_GROUPED_SELECT:
            return new JdbcSQLSyntaxErrorException(message, sql, state, errorCode, cause, stackTrace);
        case HEX_STRING_ODD_1:
        case HEX_STRING_WRONG_1:
        case INVALID_VALUE_2:
        case SEQUENCE_ATTRIBUTES_INVALID:
        case INVALID_TO_CHAR_FORMAT:
        case PARAMETER_NOT_SET_1:
        case PARSE_ERROR_1:
        case INVALID_TO_DATE_FORMAT:
        case STRING_FORMAT_ERROR_1:
        case SERIALIZATION_FAILED_1:
        case DESERIALIZATION_FAILED_1:
        case SCALAR_SUBQUERY_CONTAINS_MORE_THAN_ONE_ROW:
        case STEP_SIZE_MUST_NOT_BE_ZERO:
            return new JdbcSQLDataException(message, sql, state, errorCode, cause, stackTrace);
        case URL_RELATIVE_TO_CWD:
        case DATABASE_NOT_FOUND_1:
        case DATABASE_NOT_FOUND_2:
        case TRACE_CONNECTION_NOT_CLOSED:
        case DATABASE_ALREADY_OPEN_1:
        case FILE_CORRUPTED_1:
        case URL_FORMAT_ERROR_2:
        case DRIVER_VERSION_ERROR_2:
        case FILE_VERSION_ERROR_1:
        case FILE_ENCRYPTION_ERROR_1:
        case WRONG_PASSWORD_FORMAT:
        case UNSUPPORTED_CIPHER:
        case UNSUPPORTED_LOCK_METHOD_1:
        case EXCEPTION_OPENING_PORT_2:
        case DUPLICATE_PROPERTY_1:
        case CONNECTION_BROKEN_1:
        case UNKNOWN_MODE_1:
        case CLUSTER_ERROR_DATABASE_RUNS_ALONE:
        case CLUSTER_ERROR_DATABASE_RUNS_CLUSTERED_1:
        case DATABASE_IS_CLOSED:
        case ERROR_SETTING_DATABASE_EVENT_LISTENER_2:
        case OUT_OF_MEMORY:
        case UNSUPPORTED_SETTING_1:
        case REMOTE_CONNECTION_NOT_ALLOWED:
        case DATABASE_CALLED_AT_SHUTDOWN:
        case CANNOT_CHANGE_SETTING_WHEN_OPEN_1:
        case DATABASE_IS_IN_EXCLUSIVE_MODE:
        case INVALID_DATABASE_NAME_1:
        case AUTHENTICATOR_NOT_AVAILABLE:
            return new JdbcSQLNonTransientConnectionException(message, sql, state, errorCode, cause, stackTrace);
        case ROW_NOT_FOUND_WHEN_DELETING_1:
        case CONCURRENT_UPDATE_1:
        case ROW_NOT_FOUND_IN_PRIMARY_INDEX:
            return new JdbcSQLTransientException(message, sql, state, errorCode, cause, stackTrace);
        }
        // Default
        return new JdbcSQLException(message, sql, state, errorCode, cause, stackTrace);
    }

    private static String filterSQL(String sql) {
        return sql == null || !sql.contains(HIDE_SQL) ? sql : "-";
    }

    /**
     * Convert an exception to an IO exception.
     *
     * @param e the root cause
     * @return the IO exception
     */
    public static IOException convertToIOException(Throwable e) {
        if (e instanceof IOException) {
            return (IOException) e;
        }
        if (e instanceof JdbcException) {
            if (e.getCause() != null) {
                e = e.getCause();
            }
        }
        return new IOException(e.toString(), e);
    }

    /**
     * Builds message for an exception.
     *
     * @param e exception
     * @return message
     */
    public static String buildMessageForException(JdbcException e) {
        String s = e.getOriginalMessage();
        StringBuilder buff = new StringBuilder(s != null ? s : "- ");
        s = e.getSQL();
        if (s != null) {
            buff.append("; SQL statement:\n").append(s);
        }
        buff.append(" [").append(e.getErrorCode()).append('-').append(Constants.BUILD_ID).append(']');
        return buff.toString();
    }

    /**
     * Prints up to 100 next exceptions for a specified SQL exception.
     *
     * @param e SQL exception
     * @param s print writer
     */
    public static void printNextExceptions(SQLException e, PrintWriter s) {
        // getNextException().printStackTrace(s) would be very slow
        // if many exceptions are joined
        int i = 0;
        while ((e = e.getNextException()) != null) {
            if (i++ == 100) {
                s.println("(truncated)");
                return;
            }
            s.println(e.toString());
        }
    }

    /**
     * Prints up to 100 next exceptions for a specified SQL exception.
     *
     * @param e SQL exception
     * @param s print stream
     */
    public static void printNextExceptions(SQLException e, PrintStream s) {
        // getNextException().printStackTrace(s) would be very slow
        // if many exceptions are joined
        int i = 0;
        while ((e = e.getNextException()) != null) {
            if (i++ == 100) {
                s.println("(truncated)");
                return;
            }
            s.println(e.toString());
        }
    }

    public Object getSource() {
        return source;
    }

    public void setSource(Object source) {
        this.source = source;
    }

    /**
     * Write the exception to the driver manager log writer if configured.
     *
     * @param e the exception
     */
    public static void traceThrowable(Throwable e) {
        PrintWriter writer = DriverManager.getLogWriter();
        if (writer != null) {
            e.printStackTrace(writer);
        }
    }

}
