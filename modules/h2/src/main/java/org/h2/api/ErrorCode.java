/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.api;

/**
 * This class defines the error codes used for SQL exceptions.
 * Error messages are formatted as follows:
 * <pre>
 * { error message (possibly translated; may include quoted data) }
 * { error message in English if different }
 * { SQL statement if applicable }
 * { [ error code - build number ] }
 * </pre>
 * Example:
 * <pre>
 * Syntax error in SQL statement "SELECT * FORM[*] TEST ";
 * SQL statement: select * form test [42000-125]
 * </pre>
 * The [*] marks the position of the syntax error
 * (FORM instead of FROM in this case).
 * The error code is 42000, and the build number is 125,
 * meaning version 1.2.125.
 */
public class ErrorCode {

    // 02: no data

    /**
     * The error with code <code>2000</code> is thrown when
     * the result set is positioned before the first or after the last row, or
     * not on a valid row for the given operation.
     * Example of wrong usage:
     * <pre>
     * ResultSet rs = stat.executeQuery("SELECT * FROM DUAL");
     * rs.getString(1);
     * </pre>
     * Correct:
     * <pre>
     * ResultSet rs = stat.executeQuery("SELECT * FROM DUAL");
     * rs.next();
     * rs.getString(1);
     * </pre>
     */
    public static final int NO_DATA_AVAILABLE = 2000;

    // 07: dynamic SQL error

    /**
     * The error with code <code>7001</code> is thrown when
     * trying to call a function with the wrong number of parameters.
     * Example:
     * <pre>
     * CALL ABS(1, 2)
     * </pre>
     */
    public static final int INVALID_PARAMETER_COUNT_2 = 7001;

    // 08: connection exception

    /**
     * The error with code <code>8000</code> is thrown when
     * there was a problem trying to create a database lock.
     * See the message and cause for details.
     */
    public static final int ERROR_OPENING_DATABASE_1 = 8000;

    // 21: cardinality violation

    /**
     * The error with code <code>21002</code> is thrown when the number of
     * columns does not match. Possible reasons are: for an INSERT or MERGE
     * statement, the column count does not match the table or the column list
     * specified. For a SELECT UNION statement, both queries return a different
     * number of columns. For a constraint, the number of referenced and
     * referencing columns does not match. Example:
     * <pre>
     * CREATE TABLE TEST(ID INT, NAME VARCHAR);
     * INSERT INTO TEST VALUES('Hello');
     * </pre>
     */
    public static final int COLUMN_COUNT_DOES_NOT_MATCH = 21002;

    // 22: data exception

    /**
     * The error with code <code>22001</code> is thrown when
     * trying to insert a value that is too long for the column.
     * Example:
     * <pre>
     * CREATE TABLE TEST(ID INT, NAME VARCHAR(2));
     * INSERT INTO TEST VALUES(1, 'Hello');
     * </pre>
     */
    public static final int VALUE_TOO_LONG_2 = 22001;

    /**
     * The error with code <code>22003</code> is thrown when a value is out of
     * range when converting to another data type. Example:
     * <pre>
     * CALL CAST(1000000 AS TINYINT);
     * SELECT CAST(124.34 AS DECIMAL(2, 2));
     * </pre>
     */
    public static final int NUMERIC_VALUE_OUT_OF_RANGE_1 = 22003;

    /**
     * The error with code <code>22004</code> is thrown when a value is out of
     * range when converting to another column's data type.
     */
    public static final int NUMERIC_VALUE_OUT_OF_RANGE_2 = 22004;

    /**
     * The error with code <code>22007</code> is thrown when
     * a text can not be converted to a date, time, or timestamp constant.
     * Examples:
     * <pre>
     * CALL DATE '2007-January-01';
     * CALL TIME '14:61:00';
     * CALL TIMESTAMP '2001-02-30 12:00:00';
     * </pre>
     */
    public static final int INVALID_DATETIME_CONSTANT_2 = 22007;

    /**
     * The error with code <code>22012</code> is thrown when trying to divide
     * a value by zero. Example:
     * <pre>
     * CALL 1/0;
     * </pre>
     */
    public static final int DIVISION_BY_ZERO_1 = 22012;

    /**
     * The error with code <code>22018</code> is thrown when
     * trying to convert a value to a data type where the conversion is
     * undefined, or when an error occurred trying to convert. Example:
     * <pre>
     * CALL CAST(DATE '2001-01-01' AS BOOLEAN);
     * CALL CAST('CHF 99.95' AS INT);
     * </pre>
     */
    public static final int DATA_CONVERSION_ERROR_1 = 22018;

    /**
     * The error with code <code>22025</code> is thrown when using an invalid
     * escape character sequence for LIKE or REGEXP. The default escape
     * character is '\'. The escape character is required when searching for
     * the characters '%', '_' and the escape character itself. That means if
     * you want to search for the text '10%', you need to use LIKE '10\%'. If
     * you want to search for 'C:\temp' you need to use 'C:\\temp'. The escape
     * character can be changed using the ESCAPE clause as in LIKE '10+%' ESCAPE
     * '+'. Example of wrong usage:
     * <pre>
     * CALL 'C:\temp' LIKE 'C:\temp';
     * CALL '1+1' LIKE '1+1' ESCAPE '+';
     * </pre>
     * Correct:
     * <pre>
     * CALL 'C:\temp' LIKE 'C:\\temp';
     * CALL '1+1' LIKE '1++1' ESCAPE '+';
     * </pre>
     */
    public static final int LIKE_ESCAPE_ERROR_1 = 22025;

    /**
     * The error with code <code>22030</code> is thrown when
     * an attempt is made to INSERT or UPDATE an ENUM-typed cell,
     * but the value is not one of the values enumerated by the
     * type.
     *
     * Example:
     * <pre>
     * CREATE TABLE TEST(CASE ENUM('sensitive','insensitive'));
     * INSERT INTO TEST VALUES('snake');
     * </pre>
     */
    public static final int ENUM_VALUE_NOT_PERMITTED = 22030;

    /**
     * The error with code <code>22032</code> is thrown when an
     * attempt is made to add or modify an ENUM-typed column so
     * that one or more of its enumerators would be empty.
     *
     * Example:
     * <pre>
     * CREATE TABLE TEST(CASE ENUM(' '));
     * </pre>
     */
    public static final int ENUM_EMPTY = 22032;

    /**
     * The error with code <code>22033</code> is thrown when an
     * attempt is made to add or modify an ENUM-typed column so
     * that it would have duplicate values.
     *
     * Example:
     * <pre>
     * CREATE TABLE TEST(CASE ENUM('sensitive', 'sensitive'));
     * </pre>
     */
    public static final int ENUM_DUPLICATE = 22033;

    // 23: constraint violation

    /**
     * The error with code <code>23502</code> is thrown when
     * trying to insert NULL into a column that does not allow NULL.
     * Example:
     * <pre>
     * CREATE TABLE TEST(ID INT, NAME VARCHAR NOT NULL);
     * INSERT INTO TEST(ID) VALUES(1);
     * </pre>
     */
    public static final int NULL_NOT_ALLOWED = 23502;

    /**
     * The error with code <code>23503</code> is thrown when trying to delete
     * or update a row when this would violate a referential constraint, because
     * there is a child row that would become an orphan. Example:
     * <pre>
     * CREATE TABLE TEST(ID INT PRIMARY KEY, PARENT INT);
     * INSERT INTO TEST VALUES(1, 1), (2, 1);
     * ALTER TABLE TEST ADD CONSTRAINT TEST_ID_PARENT
     *       FOREIGN KEY(PARENT) REFERENCES TEST(ID) ON DELETE RESTRICT;
     * DELETE FROM TEST WHERE ID = 1;
     * </pre>
     */
    public static final int REFERENTIAL_INTEGRITY_VIOLATED_CHILD_EXISTS_1 = 23503;

    /**
     * The error with code <code>23505</code> is thrown when trying to insert
     * a row that would violate a unique index or primary key. Example:
     * <pre>
     * CREATE TABLE TEST(ID INT PRIMARY KEY);
     * INSERT INTO TEST VALUES(1);
     * INSERT INTO TEST VALUES(1);
     * </pre>
     */
    public static final int DUPLICATE_KEY_1 = 23505;

    /**
     * The error with code <code>23506</code> is thrown when trying to insert
     * or update a row that would violate a referential constraint, because the
     * referenced row does not exist. Example:
     * <pre>
     * CREATE TABLE PARENT(ID INT PRIMARY KEY);
     * CREATE TABLE CHILD(P_ID INT REFERENCES PARENT(ID));
     * INSERT INTO CHILD VALUES(1);
     * </pre>
     */
    public static final int REFERENTIAL_INTEGRITY_VIOLATED_PARENT_MISSING_1 = 23506;

    /**
     * The error with code <code>23507</code> is thrown when
     * updating or deleting from a table with a foreign key constraint
     * that should set the default value, but there is no default value defined.
     * Example:
     * <pre>
     * CREATE TABLE TEST(ID INT PRIMARY KEY, PARENT INT);
     * INSERT INTO TEST VALUES(1, 1), (2, 1);
     * ALTER TABLE TEST ADD CONSTRAINT TEST_ID_PARENT
     *   FOREIGN KEY(PARENT) REFERENCES TEST(ID) ON DELETE SET DEFAULT;
     * DELETE FROM TEST WHERE ID = 1;
     * </pre>
     */
    public static final int NO_DEFAULT_SET_1 = 23507;

    /**
     * The error with code <code>23513</code> is thrown when
     * a check constraint is violated. Example:
     * <pre>
     * CREATE TABLE TEST(ID INT CHECK ID&gt;0);
     * INSERT INTO TEST VALUES(0);
     * </pre>
     */
    public static final int CHECK_CONSTRAINT_VIOLATED_1 = 23513;

    /**
     * The error with code <code>23514</code> is thrown when
     * evaluation of a check constraint resulted in a error.
     */
    public static final int CHECK_CONSTRAINT_INVALID = 23514;

    // 28: invalid authorization specification

    /**
     * The error with code <code>28000</code> is thrown when
     * there is no such user registered in the database, when the user password
     * does not match, or when the database encryption password does not match
     * (if database encryption is used).
     */
    public static final int WRONG_USER_OR_PASSWORD = 28000;

    // 3B: savepoint exception

    /**
     * The error with code <code>40001</code> is thrown when
     * the database engine has detected a deadlock. The transaction of this
     * session has been rolled back to solve the problem. A deadlock occurs when
     * a session tries to lock a table another session has locked, while the
     * other session wants to lock a table the first session has locked. As an
     * example, session 1 has locked table A, while session 2 has locked table
     * B. If session 1 now tries to lock table B and session 2 tries to lock
     * table A, a deadlock has occurred. Deadlocks that involve more than two
     * sessions are also possible. To solve deadlock problems, an application
     * should lock tables always in the same order, such as always lock table A
     * before locking table B. For details, see <a
     * href="http://en.wikipedia.org/wiki/Deadlock">Wikipedia Deadlock</a>.
     */
    public static final int DEADLOCK_1 = 40001;

    // 42: syntax error or access rule violation

    /**
     * The error with code <code>42000</code> is thrown when
     * trying to execute an invalid SQL statement.
     * Example:
     * <pre>
     * CREATE ALIAS REMAINDER FOR "IEEEremainder";
     * </pre>
     */
    public static final int SYNTAX_ERROR_1 = 42000;

    /**
     * The error with code <code>42001</code> is thrown when
     * trying to execute an invalid SQL statement.
     * Example:
     * <pre>
     * CREATE TABLE TEST(ID INT);
     * INSERT INTO TEST(1);
     * </pre>
     */
    public static final int SYNTAX_ERROR_2 = 42001;

    /**
     * The error with code <code>42101</code> is thrown when
     * trying to create a table or view if an object with this name already
     * exists. Example:
     * <pre>
     * CREATE TABLE TEST(ID INT);
     * CREATE TABLE TEST(ID INT PRIMARY KEY);
     * </pre>
     */
    public static final int TABLE_OR_VIEW_ALREADY_EXISTS_1 = 42101;

    /**
     * The error with code <code>42102</code> is thrown when
     * trying to query, modify or drop a table or view that does not exists
     * in this schema and database. A common cause is that the wrong
     * database was opened.
     * Example:
     * <pre>
     * SELECT * FROM ABC;
     * </pre>
     */
    public static final int TABLE_OR_VIEW_NOT_FOUND_1 = 42102;

    /**
     * The error with code <code>42111</code> is thrown when
     * trying to create an index if an index with the same name already exists.
     * Example:
     * <pre>
     * CREATE TABLE TEST(ID INT, NAME VARCHAR);
     * CREATE INDEX IDX_ID ON TEST(ID);
     * CREATE TABLE ADDRESS(ID INT);
     * CREATE INDEX IDX_ID ON ADDRESS(ID);
     * </pre>
     */
    public static final int INDEX_ALREADY_EXISTS_1 = 42111;

    /**
     * The error with code <code>42112</code> is thrown when
     * trying to drop or reference an index that does not exist.
     * Example:
     * <pre>
     * DROP INDEX ABC;
     * </pre>
     */
    public static final int INDEX_NOT_FOUND_1 = 42112;

    /**
     * The error with code <code>42121</code> is thrown when trying to create
     * a table or insert into a table and use the same column name twice.
     * Example:
     * <pre>
     * CREATE TABLE TEST(ID INT, ID INT);
     * </pre>
     */
    public static final int DUPLICATE_COLUMN_NAME_1 = 42121;

    /**
     * The error with code <code>42122</code> is thrown when
     * referencing an non-existing column.
     * Example:
     * <pre>
     * CREATE TABLE TEST(ID INT);
     * SELECT NAME FROM TEST;
     * </pre>
     */
    public static final int COLUMN_NOT_FOUND_1 = 42122;

    // 0A: feature not supported

    // HZ: remote database access

    //

    /**
     * The error with code <code>50000</code> is thrown when
     * something unexpected occurs, for example an internal stack
     * overflow. For details about the problem, see the cause of the
     * exception in the stack trace.
     */
    public static final int GENERAL_ERROR_1 = 50000;

    /**
     * The error with code <code>50004</code> is thrown when
     * creating a table with an unsupported data type, or
     * when the data type is unknown because parameters are used.
     * Example:
     * <pre>
     * CREATE TABLE TEST(ID VERYSMALLINT);
     * </pre>
     */
    public static final int UNKNOWN_DATA_TYPE_1 = 50004;

    /**
     * The error with code <code>50100</code> is thrown when calling an
     * unsupported JDBC method or database feature. See the stack trace for
     * details.
     */
    public static final int FEATURE_NOT_SUPPORTED_1 = 50100;

    /**
     * The error with code <code>50200</code> is thrown when
     * another connection locked an object longer than the lock timeout
     * set for this connection, or when a deadlock occurred.
     * Example:
     * <pre>
     * CREATE TABLE TEST(ID INT);
     * -- connection 1:
     * SET AUTOCOMMIT FALSE;
     * INSERT INTO TEST VALUES(1);
     * -- connection 2:
     * SET AUTOCOMMIT FALSE;
     * INSERT INTO TEST VALUES(1);
     * </pre>
     */
    public static final int LOCK_TIMEOUT_1 = 50200;

    /**
     * The error with code <code>57014</code> is thrown when
     * a statement was canceled using Statement.cancel() or
     * when the query timeout has been reached.
     * Examples:
     * <pre>
     * stat.setQueryTimeout(1);
     * stat.cancel();
     * </pre>
     */
    public static final int STATEMENT_WAS_CANCELED = 57014;

    /**
     * The error with code <code>90000</code> is thrown when
     * a function that does not return a result set was used in the FROM clause.
     * Example:
     * <pre>
     * SELECT * FROM SIN(1);
     * </pre>
     */
    public static final int FUNCTION_MUST_RETURN_RESULT_SET_1 = 90000;

    /**
     * The error with code <code>90001</code> is thrown when
     * Statement.executeUpdate() was called for a SELECT statement.
     * This is not allowed according to the JDBC specs.
     */
    public static final int METHOD_NOT_ALLOWED_FOR_QUERY = 90001;

    /**
     * The error with code <code>90002</code> is thrown when
     * Statement.executeQuery() was called for a statement that does
     * not return a result set (for example, an UPDATE statement).
     * This is not allowed according to the JDBC specs.
     */
    public static final int METHOD_ONLY_ALLOWED_FOR_QUERY = 90002;

    /**
     * The error with code <code>90003</code> is thrown when
     * trying to convert a String to a binary value. Two hex digits
     * per byte are required. Example of wrong usage:
     * <pre>
     * CALL X'00023';
     * Hexadecimal string with odd number of characters: 00023
     * </pre>
     * Correct:
     * <pre>
     * CALL X'000023';
     * </pre>
     */
    public static final int HEX_STRING_ODD_1 = 90003;

    /**
     * The error with code <code>90004</code> is thrown when
     * trying to convert a text to binary, but the expression contains
     * a non-hexadecimal character.
     * Example:
     * <pre>
     * CALL X'ABCDEFGH';
     * CALL CAST('ABCDEFGH' AS BINARY);
     * </pre>
     * Conversion from text to binary is supported, but the text must
     * represent the hexadecimal encoded bytes.
     */
    public static final int HEX_STRING_WRONG_1 = 90004;

    /**
     * The error with code <code>90005</code> is thrown when
     * trying to create a trigger and using the combination of SELECT
     * and FOR EACH ROW, which we do not support.
     */
    public static final int TRIGGER_SELECT_AND_ROW_BASED_NOT_SUPPORTED = 90005;

    /**
     * The error with code <code>90006</code> is thrown when
     * trying to get a value from a sequence that has run out of numbers
     * and does not have cycling enabled.
     */
    public static final int SEQUENCE_EXHAUSTED = 90006;

    /**
     * The error with code <code>90007</code> is thrown when
     * trying to call a JDBC method on an object that has been closed.
     */
    public static final int OBJECT_CLOSED = 90007;

    /**
     * The error with code <code>90008</code> is thrown when
     * trying to use a value that is not valid for the given operation.
     * Example:
     * <pre>
     * CREATE SEQUENCE TEST INCREMENT 0;
     * </pre>
     */
    public static final int INVALID_VALUE_2 = 90008;

    /**
     * The error with code <code>90009</code> is thrown when
     * trying to create a sequence with an invalid combination
     * of attributes (min value, max value, start value, etc).
     */
    public static final int SEQUENCE_ATTRIBUTES_INVALID = 90009;

    /**
     * The error with code <code>90010</code> is thrown when
     * trying to format a timestamp or number using TO_CHAR
     * with an invalid format.
     */
    public static final int INVALID_TO_CHAR_FORMAT = 90010;

    /**
     * The error with code <code>90011</code> is thrown when
     * trying to open a connection to a database using an implicit relative
     * path, such as "jdbc:h2:test" (in which case the database file would be
     * stored in the current working directory of the application). This is not
     * allowed because it can lead to confusion where the database file is, and
     * can result in multiple databases because different working directories
     * are used. Instead, use "jdbc:h2:~/name" (relative to the current user
     * home directory), use an absolute path, set the base directory (baseDir),
     * use "jdbc:h2:./name" (explicit relative path), or set the system property
     * "h2.implicitRelativePath" to "true" (to prevent this check). For Windows,
     * an absolute path also needs to include the drive ("C:/..."). Please see
     * the documentation on the supported URL format. Example:
     * <pre>
     * jdbc:h2:test
     * </pre>
     */
    public static final int URL_RELATIVE_TO_CWD = 90011;

    /**
     * The error with code <code>90012</code> is thrown when
     * trying to execute a statement with an parameter.
     * Example:
     * <pre>
     * CALL SIN(?);
     * </pre>
     */
    public static final int PARAMETER_NOT_SET_1 = 90012;

    /**
     * The error with code <code>90013</code> is thrown when
     * trying to open a database that does not exist using the flag
     * IFEXISTS=TRUE, or when trying to access a database object with a catalog
     * name that does not match the database name. Example:
     * <pre>
     * CREATE TABLE TEST(ID INT);
     * SELECT XYZ.PUBLIC.TEST.ID FROM TEST;
     * </pre>
     */
    public static final int DATABASE_NOT_FOUND_1 = 90013;

    /**
     * The error with code <code>90014</code> is thrown when
     * trying to parse a date with an unsupported format string, or
     * when the date can not be parsed.
     * Example:
     * <pre>
     * CALL PARSEDATETIME('2001 January', 'yyyy mm');
     * </pre>
     */
    public static final int PARSE_ERROR_1 = 90014;

    /**
     * The error with code <code>90015</code> is thrown when
     * using an aggregate function with a data type that is not supported.
     * Example:
     * <pre>
     * SELECT SUM('Hello') FROM DUAL;
     * </pre>
     */
    public static final int SUM_OR_AVG_ON_WRONG_DATATYPE_1 = 90015;

    /**
     * The error with code <code>90016</code> is thrown when
     * a column was used in the expression list or the order by clause of a
     * group or aggregate query, and that column is not in the GROUP BY clause.
     * Example of wrong usage:
     * <pre>
     * CREATE TABLE TEST(ID INT, NAME VARCHAR);
     * INSERT INTO TEST VALUES(1, 'Hello'), (2, 'World');
     * SELECT ID, MAX(NAME) FROM TEST;
     * Column ID must be in the GROUP BY list.
     * </pre>
     * Correct:
     * <pre>
     * SELECT ID, MAX(NAME) FROM TEST GROUP BY ID;
     * </pre>
     */
    public static final int MUST_GROUP_BY_COLUMN_1 = 90016;

    /**
     * The error with code <code>90017</code> is thrown when
     * trying to define a second primary key constraint for this table.
     * Example:
     * <pre>
     * CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR);
     * ALTER TABLE TEST ADD CONSTRAINT PK PRIMARY KEY(NAME);
     * </pre>
     */
    public static final int SECOND_PRIMARY_KEY = 90017;

    /**
     * The error with code <code>90018</code> is thrown when
     * the connection was opened, but never closed. In the finalizer of the
     * connection, this forgotten close was detected and the connection was
     * closed automatically, but relying on the finalizer is not good practice
     * as it is not guaranteed and behavior is virtual machine dependent. The
     * application should close the connection. This exception only appears in
     * the .trace.db file. Example of wrong usage:
     * <pre>
     * Connection conn;
     * conn = DriverManager.getConnection(&quot;jdbc:h2:&tilde;/test&quot;);
     * conn = null;
     * The connection was not closed by the application and is
     * garbage collected
     * </pre>
     * Correct:
     * <pre>
     * conn.close();
     * </pre>
     */
    public static final int TRACE_CONNECTION_NOT_CLOSED = 90018;

    /**
     * The error with code <code>90019</code> is thrown when
     * trying to drop the current user, if there are no other admin users.
     * Example:
     * <pre>
     * DROP USER SA;
     * </pre>
     */
    public static final int CANNOT_DROP_CURRENT_USER = 90019;

    /**
     * The error with code <code>90020</code> is thrown when trying to open a
     * database in embedded mode if this database is already in use in another
     * process (or in a different class loader). Multiple connections to the
     * same database are supported in the following cases:
     * <ul><li>In embedded mode (URL of the form jdbc:h2:~/test) if all
     * connections are opened within the same process and class loader.
     * </li><li>In server and cluster mode (URL of the form
     * jdbc:h2:tcp://localhost/test) using remote connections.
     * </li></ul>
     * The mixed mode is also supported. This mode requires to start a server
     * in the same process where the database is open in embedded mode.
     */
    public static final int DATABASE_ALREADY_OPEN_1 = 90020;

    /**
     * The error with code <code>90021</code> is thrown when
     * trying to change a specific database property that conflicts with other
     * database properties.
     */
    public static final int UNSUPPORTED_SETTING_COMBINATION = 90021;

    /**
     * The error with code <code>90022</code> is thrown when
     * trying to call a unknown function.
     * Example:
     * <pre>
     * CALL SPECIAL_SIN(10);
     * </pre>
     */
    public static final int FUNCTION_NOT_FOUND_1 = 90022;

    /**
     * The error with code <code>90023</code> is thrown when
     * trying to set a primary key on a nullable column.
     * Example:
     * <pre>
     * CREATE TABLE TEST(ID INT, NAME VARCHAR);
     * ALTER TABLE TEST ADD CONSTRAINT PK PRIMARY KEY(ID);
     * </pre>
     */
    public static final int COLUMN_MUST_NOT_BE_NULLABLE_1 = 90023;

    /**
     * The error with code <code>90024</code> is thrown when
     * a file could not be renamed.
     */
    public static final int FILE_RENAME_FAILED_2 = 90024;

    /**
     * The error with code <code>90025</code> is thrown when
     * a file could not be deleted, because it is still in use
     * (only in Windows), or because an error occurred when deleting.
     */
    public static final int FILE_DELETE_FAILED_1 = 90025;

    /**
     * The error with code <code>90026</code> is thrown when
     * an object could not be serialized.
     */
    public static final int SERIALIZATION_FAILED_1 = 90026;

    /**
     * The error with code <code>90027</code> is thrown when
     * an object could not be de-serialized.
     */
    public static final int DESERIALIZATION_FAILED_1 = 90027;

    /**
     * The error with code <code>90028</code> is thrown when
     * an input / output error occurred. For more information, see the root
     * cause of the exception.
     */
    public static final int IO_EXCEPTION_1 = 90028;

    /**
     * The error with code <code>90029</code> is thrown when
     * calling ResultSet.deleteRow(), insertRow(), or updateRow()
     * when the current row is not updatable.
     * Example:
     * <pre>
     * ResultSet rs = stat.executeQuery("SELECT * FROM TEST");
     * rs.next();
     * rs.insertRow();
     * </pre>
     */
    public static final int NOT_ON_UPDATABLE_ROW = 90029;

    /**
     * The error with code <code>90030</code> is thrown when
     * the database engine has detected a checksum mismatch in the data
     * or index. To solve this problem, restore a backup or use the
     * Recovery tool (org.h2.tools.Recover).
     */
    public static final int FILE_CORRUPTED_1 = 90030;

    /**
     * The error with code <code>90031</code> is thrown when
     * an input / output error occurred. For more information, see the root
     * cause of the exception.
     */
    public static final int IO_EXCEPTION_2 = 90031;

    /**
     * The error with code <code>90032</code> is thrown when
     * trying to drop or alter a user that does not exist.
     * Example:
     * <pre>
     * DROP USER TEST_USER;
     * </pre>
     */
    public static final int USER_NOT_FOUND_1 = 90032;

    /**
     * The error with code <code>90033</code> is thrown when
     * trying to create a user or role if a user with this name already exists.
     * Example:
     * <pre>
     * CREATE USER TEST_USER;
     * CREATE USER TEST_USER;
     * </pre>
     */
    public static final int USER_ALREADY_EXISTS_1 = 90033;

    /**
     * The error with code <code>90034</code> is thrown when
     * writing to the trace file failed, for example because the there
     * is an I/O exception. This message is printed to System.out,
     * but only once.
     */
    public static final int TRACE_FILE_ERROR_2 = 90034;

    /**
     * The error with code <code>90035</code> is thrown when
     * trying to create a sequence if a sequence with this name already
     * exists.
     * Example:
     * <pre>
     * CREATE SEQUENCE TEST_SEQ;
     * CREATE SEQUENCE TEST_SEQ;
     * </pre>
     */
    public static final int SEQUENCE_ALREADY_EXISTS_1 = 90035;

    /**
     * The error with code <code>90036</code> is thrown when
     * trying to access a sequence that does not exist.
     * Example:
     * <pre>
     * SELECT NEXT VALUE FOR SEQUENCE XYZ;
     * </pre>
     */
    public static final int SEQUENCE_NOT_FOUND_1 = 90036;

    /**
     * The error with code <code>90037</code> is thrown when
     * trying to drop or alter a view that does not exist.
     * Example:
     * <pre>
     * DROP VIEW XYZ;
     * </pre>
     */
    public static final int VIEW_NOT_FOUND_1 = 90037;

    /**
     * The error with code <code>90038</code> is thrown when
     * trying to create a view if a view with this name already
     * exists.
     * Example:
     * <pre>
     * CREATE VIEW DUMMY AS SELECT * FROM DUAL;
     * CREATE VIEW DUMMY AS SELECT * FROM DUAL;
     * </pre>
     */
    public static final int VIEW_ALREADY_EXISTS_1 = 90038;

    /**
     * The error with code <code>90039</code> is thrown when
     * trying to access a CLOB or BLOB object that timed out.
     * See the database setting LOB_TIMEOUT.
     */
    public static final int LOB_CLOSED_ON_TIMEOUT_1 = 90039;

    /**
     * The error with code <code>90040</code> is thrown when
     * a user that is not administrator tries to execute a statement
     * that requires admin privileges.
     */
    public static final int ADMIN_RIGHTS_REQUIRED = 90040;

    /**
     * The error with code <code>90041</code> is thrown when
     * trying to create a trigger and there is already a trigger with that name.
     * <pre>
     * CREATE TABLE TEST(ID INT);
     * CREATE TRIGGER TRIGGER_A AFTER INSERT ON TEST
     *      CALL "org.h2.samples.TriggerSample$MyTrigger";
     * CREATE TRIGGER TRIGGER_A AFTER INSERT ON TEST
     *      CALL "org.h2.samples.TriggerSample$MyTrigger";
     * </pre>
     */
    public static final int TRIGGER_ALREADY_EXISTS_1 = 90041;

    /**
     * The error with code <code>90042</code> is thrown when
     * trying to drop a trigger that does not exist.
     * Example:
     * <pre>
     * DROP TRIGGER TRIGGER_XYZ;
     * </pre>
     */
    public static final int TRIGGER_NOT_FOUND_1 = 90042;

    /**
     * The error with code <code>90043</code> is thrown when
     * there is an error initializing the trigger, for example because the
     * class does not implement the Trigger interface.
     * See the root cause for details.
     * Example:
     * <pre>
     * CREATE TABLE TEST(ID INT);
     * CREATE TRIGGER TRIGGER_A AFTER INSERT ON TEST
     *      CALL "java.lang.String";
     * </pre>
     */
    public static final int ERROR_CREATING_TRIGGER_OBJECT_3 = 90043;

    /**
     * The error with code <code>90044</code> is thrown when
     * an exception or error occurred while calling the triggers fire method.
     * See the root cause for details.
     */
    public static final int ERROR_EXECUTING_TRIGGER_3 = 90044;

    /**
     * The error with code <code>90045</code> is thrown when trying to create a
     * constraint if an object with this name already exists. Example:
     * <pre>
     * CREATE TABLE TEST(ID INT NOT NULL);
     * ALTER TABLE TEST ADD CONSTRAINT PK PRIMARY KEY(ID);
     * ALTER TABLE TEST ADD CONSTRAINT PK PRIMARY KEY(ID);
     * </pre>
     */
    public static final int CONSTRAINT_ALREADY_EXISTS_1 = 90045;

    /**
     * The error with code <code>90046</code> is thrown when
     * trying to open a connection to a database using an unsupported URL
     * format. Please see the documentation on the supported URL format and
     * examples. Example:
     * <pre>
     * jdbc:h2:;;
     * </pre>
     */
    public static final int URL_FORMAT_ERROR_2 = 90046;

    /**
     * The error with code <code>90047</code> is thrown when
     * trying to connect to a TCP server with an incompatible client.
     */
    public static final int DRIVER_VERSION_ERROR_2 = 90047;

    /**
     * The error with code <code>90048</code> is thrown when
     * the file header of a database files (*.db) does not match the
     * expected version, or if it is corrupted.
     */
    public static final int FILE_VERSION_ERROR_1 = 90048;

    /**
     * The error with code <code>90049</code> is thrown when
     * trying to open an encrypted database with the wrong file encryption
     * password or algorithm.
     */
    public static final int FILE_ENCRYPTION_ERROR_1 = 90049;

    /**
     * The error with code <code>90050</code> is thrown when trying to open an
     * encrypted database, but not separating the file password from the user
     * password. The file password is specified in the password field, before
     * the user password. A single space needs to be added between the file
     * password and the user password; the file password itself may not contain
     * spaces. File passwords (as well as user passwords) are case sensitive.
     * Example of wrong usage:
     * <pre>
     * String url = &quot;jdbc:h2:&tilde;/test;CIPHER=AES&quot;;
     * String passwords = &quot;filePasswordUserPassword&quot;;
     * DriverManager.getConnection(url, &quot;sa&quot;, pwds);
     * </pre>
     * Correct:
     * <pre>
     * String url = &quot;jdbc:h2:&tilde;/test;CIPHER=AES&quot;;
     * String passwords = &quot;filePassword userPassword&quot;;
     * DriverManager.getConnection(url, &quot;sa&quot;, pwds);
     * </pre>
     */
    public static final int WRONG_PASSWORD_FORMAT = 90050;

    /**
     * The error with code <code>90051</code> is thrown when
     * trying to use a scale that is > precision.
     * Example:
     * <pre>
     * CREATE TABLE TABLE1 ( FAIL NUMBER(6,24) );
     * </pre>
     */
    public static final int INVALID_VALUE_SCALE_PRECISION = 90051;

    /**
     * The error with code <code>90052</code> is thrown when
     * a subquery that is used as a value contains more than one column.
     * Example of wrong usage:
     * <pre>
     * CREATE TABLE TEST(ID INT);
     * INSERT INTO TEST VALUES(1), (2);
     * SELECT * FROM TEST WHERE ID IN (SELECT 1, 2 FROM DUAL);
     * </pre>
     * Correct:
     * <pre>
     * CREATE TABLE TEST(ID INT);
     * INSERT INTO TEST VALUES(1), (2);
     * SELECT * FROM TEST WHERE ID IN (1, 2);
     * </pre>
     */
    public static final int SUBQUERY_IS_NOT_SINGLE_COLUMN = 90052;

    /**
     * The error with code <code>90053</code> is thrown when
     * a subquery that is used as a value contains more than one row.
     * Example:
     * <pre>
     * CREATE TABLE TEST(ID INT, NAME VARCHAR);
     * INSERT INTO TEST VALUES(1, 'Hello'), (1, 'World');
     * SELECT X, (SELECT NAME FROM TEST WHERE ID=X) FROM DUAL;
     * </pre>
     */
    public static final int SCALAR_SUBQUERY_CONTAINS_MORE_THAN_ONE_ROW = 90053;

    /**
     * The error with code <code>90054</code> is thrown when
     * an aggregate function is used where it is not allowed.
     * Example:
     * <pre>
     * CREATE TABLE TEST(ID INT);
     * INSERT INTO TEST VALUES(1), (2);
     * SELECT MAX(ID) FROM TEST WHERE ID = MAX(ID) GROUP BY ID;
     * </pre>
     */
    public static final int INVALID_USE_OF_AGGREGATE_FUNCTION_1 = 90054;

    /**
     * The error with code <code>90055</code> is thrown when
     * trying to open a database with an unsupported cipher algorithm.
     * Supported is AES.
     * Example:
     * <pre>
     * jdbc:h2:~/test;CIPHER=DES
     * </pre>
     */
    public static final int UNSUPPORTED_CIPHER = 90055;

    /**
    * The error with code <code>90056</code> is thrown when trying to format a
    * timestamp using TO_DATE and TO_TIMESTAMP  with an invalid format.
    */
    public static final int INVALID_TO_DATE_FORMAT = 90056;

    /**
     * The error with code <code>90057</code> is thrown when
     * trying to drop a constraint that does not exist.
     * Example:
     * <pre>
     * CREATE TABLE TEST(ID INT);
     * ALTER TABLE TEST DROP CONSTRAINT CID;
     * </pre>
     */
    public static final int CONSTRAINT_NOT_FOUND_1 = 90057;

    /**
     * The error with code <code>90058</code> is thrown when trying to call
     * commit or rollback inside a trigger, or when trying to call a method
     * inside a trigger that implicitly commits the current transaction, if an
     * object is locked. This is not because it would release the lock too
     * early.
     */
    public static final int COMMIT_ROLLBACK_NOT_ALLOWED = 90058;

    /**
     * The error with code <code>90059</code> is thrown when
     * a query contains a column that could belong to multiple tables.
     * Example:
     * <pre>
     * CREATE TABLE PARENT(ID INT, NAME VARCHAR);
     * CREATE TABLE CHILD(PID INT, NAME VARCHAR);
     * SELECT ID, NAME FROM PARENT P, CHILD C WHERE P.ID = C.PID;
     * </pre>
     */
    public static final int AMBIGUOUS_COLUMN_NAME_1 = 90059;

    /**
     * The error with code <code>90060</code> is thrown when
     * trying to use a file locking mechanism that is not supported.
     * Currently only FILE (the default) and SOCKET are supported
     * Example:
     * <pre>
     * jdbc:h2:~/test;FILE_LOCK=LDAP
     * </pre>
     */
    public static final int UNSUPPORTED_LOCK_METHOD_1 = 90060;

    /**
     * The error with code <code>90061</code> is thrown when
     * trying to start a server if a server is already running at the same port.
     * It could also be a firewall problem. To find out if another server is
     * already running, run the following command on Windows:
     * <pre>
     * netstat -ano
     * </pre>
     * The column PID is the process id as listed in the Task Manager.
     * For Linux, use:
     * <pre>
     * netstat -npl
     * </pre>
     */
    public static final int EXCEPTION_OPENING_PORT_2 = 90061;

    /**
     * The error with code <code>90062</code> is thrown when
     * a directory or file could not be created. This can occur when
     * trying to create a directory if a file with the same name already
     * exists, or vice versa.
     *
     */
    public static final int FILE_CREATION_FAILED_1 = 90062;

    /**
     * The error with code <code>90063</code> is thrown when
     * trying to rollback to a savepoint that is not defined.
     * Example:
     * <pre>
     * ROLLBACK TO SAVEPOINT S_UNKNOWN;
     * </pre>
     */
    public static final int SAVEPOINT_IS_INVALID_1 = 90063;

    /**
     * The error with code <code>90064</code> is thrown when
     * Savepoint.getSavepointName() is called on an unnamed savepoint.
     * Example:
     * <pre>
     * Savepoint sp = conn.setSavepoint();
     * sp.getSavepointName();
     * </pre>
     */
    public static final int SAVEPOINT_IS_UNNAMED = 90064;

    /**
     * The error with code <code>90065</code> is thrown when
     * Savepoint.getSavepointId() is called on a named savepoint.
     * Example:
     * <pre>
     * Savepoint sp = conn.setSavepoint("Joe");
     * sp.getSavepointId();
     * </pre>
     */
    public static final int SAVEPOINT_IS_NAMED = 90065;

    /**
     * The error with code <code>90066</code> is thrown when
     * the same property appears twice in the database URL or in
     * the connection properties.
     * Example:
     * <pre>
     * jdbc:h2:~/test;LOCK_TIMEOUT=0;LOCK_TIMEOUT=1
     * </pre>
     */
    public static final int DUPLICATE_PROPERTY_1 = 90066;

    /**
     * The error with code <code>90067</code> is thrown when the client could
     * not connect to the database, or if the connection was lost. Possible
     * reasons are: the database server is not running at the given port, the
     * connection was closed due to a shutdown, or the server was stopped. Other
     * possible causes are: the server is not an H2 server, or the network
     * connection is broken.
     */
    public static final int CONNECTION_BROKEN_1 = 90067;

    /**
     * The error with code <code>90068</code> is thrown when the given
     * expression that is used in the ORDER BY is not in the result list. This
     * is required for distinct queries, otherwise the result would be
     * ambiguous.
     * Example of wrong usage:
     * <pre>
     * CREATE TABLE TEST(ID INT, NAME VARCHAR);
     * INSERT INTO TEST VALUES(2, 'Hello'), (1, 'Hello');
     * SELECT DISTINCT NAME FROM TEST ORDER BY ID;
     * Order by expression ID must be in the result list in this case
     * </pre>
     * Correct:
     * <pre>
     * SELECT DISTINCT ID, NAME FROM TEST ORDER BY ID;
     * </pre>
     */
    public static final int ORDER_BY_NOT_IN_RESULT = 90068;

    /**
     * The error with code <code>90069</code> is thrown when
     * trying to create a role if an object with this name already exists.
     * Example:
     * <pre>
     * CREATE ROLE TEST_ROLE;
     * CREATE ROLE TEST_ROLE;
     * </pre>
     */
    public static final int ROLE_ALREADY_EXISTS_1 = 90069;

    /**
     * The error with code <code>90070</code> is thrown when
     * trying to drop or grant a role that does not exists.
     * Example:
     * <pre>
     * DROP ROLE TEST_ROLE_2;
     * </pre>
     */
    public static final int ROLE_NOT_FOUND_1 = 90070;

    /**
     * The error with code <code>90071</code> is thrown when
     * trying to grant or revoke if no role or user with that name exists.
     * Example:
     * <pre>
     * GRANT SELECT ON TEST TO UNKNOWN;
     * </pre>
     */
    public static final int USER_OR_ROLE_NOT_FOUND_1 = 90071;

    /**
     * The error with code <code>90072</code> is thrown when
     * trying to grant or revoke both roles and rights at the same time.
     * Example:
     * <pre>
     * GRANT SELECT, TEST_ROLE ON TEST TO SA;
     * </pre>
     */
    public static final int ROLES_AND_RIGHT_CANNOT_BE_MIXED = 90072;

    /**
     * The error with code <code>90073</code> is thrown when trying to create
     * an alias for a Java method, if two methods exists in this class that have
     * this name and the same number of parameters.
     * Example of wrong usage:
     * <pre>
     * CREATE ALIAS GET_LONG FOR
     *      "java.lang.Long.getLong";
     * </pre>
     * Correct:
     * <pre>
     * CREATE ALIAS GET_LONG FOR
     *      "java.lang.Long.getLong(java.lang.String, java.lang.Long)";
     * </pre>
     */
    public static final int METHODS_MUST_HAVE_DIFFERENT_PARAMETER_COUNTS_2 = 90073;

    /**
     * The error with code <code>90074</code> is thrown when
     * trying to grant a role that has already been granted.
     * Example:
     * <pre>
     * CREATE ROLE TEST_A;
     * CREATE ROLE TEST_B;
     * GRANT TEST_A TO TEST_B;
     * GRANT TEST_B TO TEST_A;
     * </pre>
     */
    public static final int ROLE_ALREADY_GRANTED_1 = 90074;

    /**
     * The error with code <code>90075</code> is thrown when
     * trying to alter a table and allow null for a column that is part of a
     * primary key or hash index.
     * Example:
     * <pre>
     * CREATE TABLE TEST(ID INT PRIMARY KEY);
     * ALTER TABLE TEST ALTER COLUMN ID NULL;
     * </pre>
     */
    public static final int COLUMN_IS_PART_OF_INDEX_1 = 90075;

    /**
     * The error with code <code>90076</code> is thrown when
     * trying to create a function alias for a system function or for a function
     * that is already defined.
     * Example:
     * <pre>
     * CREATE ALIAS SQRT FOR "java.lang.Math.sqrt"
     * </pre>
     */
    public static final int FUNCTION_ALIAS_ALREADY_EXISTS_1 = 90076;

    /**
     * The error with code <code>90077</code> is thrown when
     * trying to drop a system function or a function alias that does not exist.
     * Example:
     * <pre>
     * DROP ALIAS SQRT;
     * </pre>
     */
    public static final int FUNCTION_ALIAS_NOT_FOUND_1 = 90077;

    /**
     * The error with code <code>90078</code> is thrown when
     * trying to create a schema if an object with this name already exists.
     * Example:
     * <pre>
     * CREATE SCHEMA TEST_SCHEMA;
     * CREATE SCHEMA TEST_SCHEMA;
     * </pre>
     */
    public static final int SCHEMA_ALREADY_EXISTS_1 = 90078;

    /**
     * The error with code <code>90079</code> is thrown when
     * trying to drop a schema that does not exist.
     * Example:
     * <pre>
     * DROP SCHEMA UNKNOWN;
     * </pre>
     */
    public static final int SCHEMA_NOT_FOUND_1 = 90079;

    /**
     * The error with code <code>90080</code> is thrown when
     * trying to rename a object to a different schema, or when trying to
     * create a related object in another schema.
     * For CREATE LINKED TABLE, it is thrown when multiple tables with that
     * name exist in different schemas.
     * Example:
     * <pre>
     * CREATE SCHEMA TEST_SCHEMA;
     * CREATE TABLE TEST(ID INT);
     * CREATE INDEX TEST_ID ON TEST(ID);
     * ALTER INDEX TEST_ID RENAME TO TEST_SCHEMA.IDX_TEST_ID;
     * </pre>
     */
    public static final int SCHEMA_NAME_MUST_MATCH = 90080;

    /**
     * The error with code <code>90081</code> is thrown when
     * trying to alter a column to not allow NULL, if there
     * is already data in the table where this column is NULL.
     * Example:
     * <pre>
     * CREATE TABLE TEST(ID INT);
     * INSERT INTO TEST VALUES(NULL);
     * ALTER TABLE TEST ALTER COLUMN ID VARCHAR NOT NULL;
     * </pre>
     */
    public static final int COLUMN_CONTAINS_NULL_VALUES_1 = 90081;

    /**
     * The error with code <code>90082</code> is thrown when
     * trying to drop a system generated sequence.
     */
    public static final int SEQUENCE_BELONGS_TO_A_TABLE_1 = 90082;

    /**
     * The error with code <code>90083</code> is thrown when
     * trying to drop a column that is part of a constraint.
     * Example:
     * <pre>
     * CREATE TABLE TEST(ID INT, PID INT REFERENCES(ID));
     * ALTER TABLE TEST DROP COLUMN PID;
     * </pre>
     */
    public static final int COLUMN_IS_REFERENCED_1 = 90083;

    /**
     * The error with code <code>90084</code> is thrown when
     * trying to drop the last column of a table.
     * Example:
     * <pre>
     * CREATE TABLE TEST(ID INT);
     * ALTER TABLE TEST DROP COLUMN ID;
     * </pre>
     */
    public static final int CANNOT_DROP_LAST_COLUMN = 90084;

    /**
     * The error with code <code>90085</code> is thrown when
     * trying to manually drop an index that was generated by the system
     * because of a unique or referential constraint. To find out what
     * constraint causes the problem, run:
     * <pre>
     * SELECT * FROM INFORMATION_SCHEMA.CONSTRAINTS
     * WHERE UNIQUE_INDEX_NAME = '&lt;index name&gt;';
     * </pre>
     * Example of wrong usage:
     * <pre>
     * CREATE TABLE TEST(ID INT, CONSTRAINT UID UNIQUE(ID));
     * DROP INDEX UID_INDEX_0;
     * Index UID_INDEX_0 belongs to constraint UID
     * </pre>
     * Correct:
     * <pre>
     * ALTER TABLE TEST DROP CONSTRAINT UID;
     * </pre>
     */
    public static final int INDEX_BELONGS_TO_CONSTRAINT_2 = 90085;

    /**
     * The error with code <code>90086</code> is thrown when
     * a class can not be loaded because it is not in the classpath
     * or because a related class is not in the classpath.
     * Example:
     * <pre>
     * CREATE ALIAS TEST FOR "java.lang.invalid.Math.sqrt";
     * </pre>
     */
    public static final int CLASS_NOT_FOUND_1 = 90086;

    /**
     * The error with code <code>90087</code> is thrown when
     * a method with matching number of arguments was not found in the class.
     * Example:
     * <pre>
     * CREATE ALIAS TO_BINARY FOR "java.lang.Long.toBinaryString(long)";
     * CALL TO_BINARY(10, 2);
     * </pre>
     */
    public static final int METHOD_NOT_FOUND_1 = 90087;

    /**
     * The error with code <code>90088</code> is thrown when
     * trying to switch to an unknown mode.
     * Example:
     * <pre>
     * SET MODE UNKNOWN;
     * </pre>
     */
    public static final int UNKNOWN_MODE_1 = 90088;

    /**
     * The error with code <code>90089</code> is thrown when
     * trying to change the collation while there was already data in
     * the database. The collation of the database must be set when the
     * database is empty.
     * Example of wrong usage:
     * <pre>
     * CREATE TABLE TEST(NAME VARCHAR PRIMARY KEY);
     * INSERT INTO TEST VALUES('Hello', 'World');
     * SET COLLATION DE;
     * Collation cannot be changed because there is a data table: PUBLIC.TEST
     * </pre>
     * Correct:
     * <pre>
     * SET COLLATION DE;
     * CREATE TABLE TEST(NAME VARCHAR PRIMARY KEY);
     * INSERT INTO TEST VALUES('Hello', 'World');
     * </pre>
     */
    public static final int COLLATION_CHANGE_WITH_DATA_TABLE_1 = 90089;

    /**
     * The error with code <code>90090</code> is thrown when
     * trying to drop a schema that may not be dropped (the schema PUBLIC
     * and the schema INFORMATION_SCHEMA).
     * Example:
     * <pre>
     * DROP SCHEMA PUBLIC;
     * </pre>
     */
    public static final int SCHEMA_CAN_NOT_BE_DROPPED_1 = 90090;

    /**
     * The error with code <code>90091</code> is thrown when
     * trying to drop the role PUBLIC.
     * Example:
     * <pre>
     * DROP ROLE PUBLIC;
     * </pre>
     */
    public static final int ROLE_CAN_NOT_BE_DROPPED_1 = 90091;

    /**
     * The error with code <code>90093</code> is thrown when
     * trying to connect to a clustered database that runs in standalone
     * mode. This can happen if clustering is not enabled on the database,
     * or if one of the clients disabled clustering because it can not see
     * the other cluster node.
     */
    public static final int CLUSTER_ERROR_DATABASE_RUNS_ALONE = 90093;

    /**
     * The error with code <code>90094</code> is thrown when
     * trying to connect to a clustered database that runs together with a
     * different cluster node setting than what is used when trying to connect.
     */
    public static final int CLUSTER_ERROR_DATABASE_RUNS_CLUSTERED_1 = 90094;

    /**
     * The error with code <code>90095</code> is thrown when
     * calling the method STRINGDECODE with an invalid escape sequence.
     * Only Java style escape sequences and Java properties file escape
     * sequences are supported.
     * Example:
     * <pre>
     * CALL STRINGDECODE('\i');
     * </pre>
     */
    public static final int STRING_FORMAT_ERROR_1 = 90095;

    /**
     * The error with code <code>90096</code> is thrown when
     * trying to perform an operation with a non-admin user if the
     * user does not have enough rights.
     */
    public static final int NOT_ENOUGH_RIGHTS_FOR_1 = 90096;

    /**
     * The error with code <code>90097</code> is thrown when
     * trying to delete or update a database if it is open in read-only mode.
     * Example:
     * <pre>
     * jdbc:h2:~/test;ACCESS_MODE_DATA=R
     * CREATE TABLE TEST(ID INT);
     * </pre>
     */
    public static final int DATABASE_IS_READ_ONLY = 90097;

    /**
     * The error with code <code>90098</code> is thrown when the database has
     * been closed, for example because the system ran out of memory or because
     * the self-destruction counter has reached zero. This counter is only used
     * for recovery testing, and not set in normal operation.
     */
    public static final int DATABASE_IS_CLOSED = 90098;

    /**
     * The error with code <code>90099</code> is thrown when an error occurred
     * trying to initialize the database event listener. Example:
     * <pre>
     * jdbc:h2:&tilde;/test;DATABASE_EVENT_LISTENER='java.lang.String'
     * </pre>
     */
    public static final int ERROR_SETTING_DATABASE_EVENT_LISTENER_2 = 90099;

    /**
     * The error with code <code>90101</code> is thrown when
     * the XA API detected unsupported transaction names. This can happen
     * when mixing application generated transaction names and transaction names
     * generated by this databases XAConnection API.
     */
    public static final int WRONG_XID_FORMAT_1 = 90101;

    /**
     * The error with code <code>90102</code> is thrown when
     * trying to use unsupported options for the given compression algorithm.
     * Example of wrong usage:
     * <pre>
     * CALL COMPRESS(STRINGTOUTF8(SPACE(100)), 'DEFLATE l 10');
     * </pre>
     * Correct:
     * <pre>
     * CALL COMPRESS(STRINGTOUTF8(SPACE(100)), 'DEFLATE l 9');
     * </pre>
     */
    public static final int UNSUPPORTED_COMPRESSION_OPTIONS_1 = 90102;

    /**
     * The error with code <code>90103</code> is thrown when
     * trying to use an unsupported compression algorithm.
     * Example:
     * <pre>
     * CALL COMPRESS(STRINGTOUTF8(SPACE(100)), 'BZIP');
     * </pre>
     */
    public static final int UNSUPPORTED_COMPRESSION_ALGORITHM_1 = 90103;

    /**
     * The error with code <code>90104</code> is thrown when
     * the data can not be de-compressed.
     * Example:
     * <pre>
     * CALL EXPAND(X'00FF');
     * </pre>
     */
    public static final int COMPRESSION_ERROR = 90104;

    /**
     * The error with code <code>90105</code> is thrown when
     * an exception occurred in a user-defined method.
     * Example:
     * <pre>
     * CREATE ALIAS SYS_PROP FOR "java.lang.System.getProperty";
     * CALL SYS_PROP(NULL);
     * </pre>
     */
    public static final int EXCEPTION_IN_FUNCTION_1 = 90105;

    /**
     * The error with code <code>90106</code> is thrown when
     * trying to truncate a table that can not be truncated.
     * Tables with referential integrity constraints can not be truncated.
     * Also, system tables and view can not be truncated.
     * Example:
     * <pre>
     * TRUNCATE TABLE INFORMATION_SCHEMA.SETTINGS;
     * </pre>
     */
    public static final int CANNOT_TRUNCATE_1 = 90106;

    /**
     * The error with code <code>90107</code> is thrown when
     * trying to drop an object because another object would become invalid.
     * Example:
     * <pre>
     * CREATE TABLE COUNT(X INT);
     * CREATE TABLE ITEMS(ID INT DEFAULT SELECT MAX(X)+1 FROM COUNT);
     * DROP TABLE COUNT;
     * </pre>
     */
    public static final int CANNOT_DROP_2 = 90107;

    /**
     * The error with code <code>90108</code> is thrown when not enough heap
     * memory was available. A possible solutions is to increase the memory size
     * using <code>java -Xmx128m ...</code>. Another solution is to reduce
     * the cache size.
     */
    public static final int OUT_OF_MEMORY = 90108;

    /**
     * The error with code <code>90109</code> is thrown when
     * trying to run a query against an invalid view.
     * Example:
     * <pre>
     * CREATE FORCE VIEW TEST_VIEW AS SELECT * FROM TEST;
     * SELECT * FROM TEST_VIEW;
     * </pre>
     */
    public static final int VIEW_IS_INVALID_2 = 90109;

    /**
     * The error with code <code>90111</code> is thrown when
     * an exception occurred while accessing a linked table.
     */
    public static final int ERROR_ACCESSING_LINKED_TABLE_2 = 90111;

    /**
     * The error with code <code>90112</code> is thrown when a row was deleted
     * twice while locking was disabled. This is an intern exception that should
     * never be thrown to the application, because such deleted should be
     * detected and the resulting exception ignored inside the database engine.
     * <pre>
     * Row not found when trying to delete from index UID_INDEX_0
     * </pre>
     */
    public static final int ROW_NOT_FOUND_WHEN_DELETING_1 = 90112;

    /**
     * The error with code <code>90113</code> is thrown when
     * the database URL contains unsupported settings.
     * Example:
     * <pre>
     * jdbc:h2:~/test;UNKNOWN=TRUE
     * </pre>
     */
    public static final int UNSUPPORTED_SETTING_1 = 90113;

    /**
     * The error with code <code>90114</code> is thrown when
     * trying to create a constant if a constant with this name already exists.
     * Example:
     * <pre>
     * CREATE CONSTANT TEST VALUE 1;
     * CREATE CONSTANT TEST VALUE 1;
     * </pre>
     */
    public static final int CONSTANT_ALREADY_EXISTS_1 = 90114;

    /**
     * The error with code <code>90115</code> is thrown when
     * trying to drop a constant that does not exists.
     * Example:
     * <pre>
     * DROP CONSTANT UNKNOWN;
     * </pre>
     */
    public static final int CONSTANT_NOT_FOUND_1 = 90115;

    /**
     * The error with code <code>90116</code> is thrown when
     * trying use a literal in a SQL statement if literals are disabled.
     * If literals are disabled, use PreparedStatement and parameters instead
     * of literals in the SQL statement.
     * Example:
     * <pre>
     * SET ALLOW_LITERALS NONE;
     * CALL 1+1;
     * </pre>
     */
    public static final int LITERALS_ARE_NOT_ALLOWED = 90116;

    /**
     * The error with code <code>90117</code> is thrown when
     * trying to connect to a TCP server from another machine, if remote
     * connections are not allowed. To allow remote connections,
     * start the TCP server using the option -tcpAllowOthers as in:
     * <pre>
     * java org.h2.tools.Server -tcp -tcpAllowOthers
     * </pre>
     * Or, when starting the server from an application, use:
     * <pre>
     * Server server = Server.createTcpServer("-tcpAllowOthers");
     * server.start();
     * </pre>
     */
    public static final int REMOTE_CONNECTION_NOT_ALLOWED = 90117;

    /**
     * The error with code <code>90118</code> is thrown when
     * trying to drop a table can not be dropped.
     * Example:
     * <pre>
     * DROP TABLE INFORMATION_SCHEMA.SETTINGS;
     * </pre>
     */
    public static final int CANNOT_DROP_TABLE_1 = 90118;

    /**
     * The error with code <code>90119</code> is thrown when
     * trying to create a domain if an object with this name already exists,
     * or when trying to overload a built-in data type.
     * Example:
     * <pre>
     * CREATE DOMAIN INTEGER AS VARCHAR;
     * CREATE DOMAIN EMAIL AS VARCHAR CHECK LOCATE('@', VALUE) > 0;
     * CREATE DOMAIN EMAIL AS VARCHAR CHECK LOCATE('@', VALUE) > 0;
     * </pre>
     */
    public static final int USER_DATA_TYPE_ALREADY_EXISTS_1 = 90119;

    /**
     * The error with code <code>90120</code> is thrown when
     * trying to drop a domain that doesn't exist.
     * Example:
     * <pre>
     * DROP DOMAIN UNKNOWN;
     * </pre>
     */
    public static final int USER_DATA_TYPE_NOT_FOUND_1 = 90120;

    /**
     * The error with code <code>90121</code> is thrown when
     * a database operation is started while the virtual machine exits
     * (for example in a shutdown hook), or when the session is closed.
     */
    public static final int DATABASE_CALLED_AT_SHUTDOWN = 90121;

    /**
     * The error with code <code>90123</code> is thrown when
     * trying mix regular parameters and indexed parameters in the same
     * statement. Example:
     * <pre>
     * SELECT ?, ?1 FROM DUAL;
     * </pre>
     */
    public static final int CANNOT_MIX_INDEXED_AND_UNINDEXED_PARAMS = 90123;

    /**
     * The error with code <code>90124</code> is thrown when
     * trying to access a file that doesn't exist. This can occur when trying to
     * read a lob if the lob file has been deleted by another application.
     */
    public static final int FILE_NOT_FOUND_1 = 90124;

    /**
     * The error with code <code>90125</code> is thrown when
     * PreparedStatement.setBigDecimal is called with object that extends the
     * class BigDecimal, and the system property h2.allowBigDecimalExtensions is
     * not set. Using extensions of BigDecimal is dangerous because the database
     * relies on the behavior of BigDecimal. Example of wrong usage:
     * <pre>
     * BigDecimal bd = new MyDecimal("$10.3");
     * prep.setBigDecimal(1, bd);
     * Invalid class, expected java.math.BigDecimal but got MyDecimal
     * </pre>
     * Correct:
     * <pre>
     * BigDecimal bd = new BigDecimal(&quot;10.3&quot;);
     * prep.setBigDecimal(1, bd);
     * </pre>
     */
    public static final int INVALID_CLASS_2 = 90125;

    /**
     * The error with code <code>90126</code> is thrown when
     * trying to call the BACKUP statement for an in-memory database.
     * Example:
     * <pre>
     * jdbc:h2:mem:
     * BACKUP TO 'test.zip';
     * </pre>
     */
    public static final int DATABASE_IS_NOT_PERSISTENT = 90126;

    /**
     * The error with code <code>90127</code> is thrown when
     * trying to update or delete a row in a result set if the result set is
     * not updatable. Result sets are only updatable if:
     * the statement was created with updatable concurrency;
     * all columns of the result set are from the same table;
     * the table is a data table (not a system table or view);
     * all columns of the primary key or any unique index are included;
     * all columns of the result set are columns of that table.
     */
    public static final int RESULT_SET_NOT_UPDATABLE = 90127;

    /**
     * The error with code <code>90128</code> is thrown when
     * trying to call a method of the ResultSet that is only supported
     * for scrollable result sets, and the result set is not scrollable.
     * Example:
     * <pre>
     * rs.first();
     * </pre>
     */
    public static final int RESULT_SET_NOT_SCROLLABLE = 90128;

    /**
     * The error with code <code>90129</code> is thrown when
     * trying to commit a transaction that doesn't exist.
     * Example:
     * <pre>
     * PREPARE COMMIT ABC;
     * COMMIT TRANSACTION TEST;
     * </pre>
     */
    public static final int TRANSACTION_NOT_FOUND_1 = 90129;

    /**
     * The error with code <code>90130</code> is thrown when
     * an execute method of PreparedStatement was called with a SQL statement.
     * This is not allowed according to the JDBC specification. Instead, use
     * an execute method of Statement.
     * Example of wrong usage:
     * <pre>
     * PreparedStatement prep = conn.prepareStatement("SELECT * FROM TEST");
     * prep.execute("DELETE FROM TEST");
     * </pre>
     * Correct:
     * <pre>
     * Statement stat = conn.createStatement();
     * stat.execute("DELETE FROM TEST");
     * </pre>
     */
    public static final int METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT = 90130;

    /**
     * The error with code <code>90131</code> is thrown when using multi version
     * concurrency control, and trying to update the same row from within two
     * connections at the same time, or trying to insert two rows with the same
     * key from two connections. Example:
     * <pre>
     * jdbc:h2:~/test;MVCC=TRUE
     * Session 1:
     * CREATE TABLE TEST(ID INT);
     * INSERT INTO TEST VALUES(1);
     * SET AUTOCOMMIT FALSE;
     * UPDATE TEST SET ID = 2;
     * Session 2:
     * SET AUTOCOMMIT FALSE;
     * UPDATE TEST SET ID = 3;
     * </pre>
     */
    public static final int CONCURRENT_UPDATE_1 = 90131;

    /**
     * The error with code <code>90132</code> is thrown when
     * trying to drop a user-defined aggregate function that doesn't exist.
     * Example:
     * <pre>
     * DROP AGGREGATE UNKNOWN;
     * </pre>
     */
    public static final int AGGREGATE_NOT_FOUND_1 = 90132;

    /**
     * The error with code <code>90133</code> is thrown when
     * trying to change a specific database property while the database is
     * already open. The MVCC property needs to be set in the first connection
     * (in the connection opening the database) and can not be changed later on.
     */
    public static final int CANNOT_CHANGE_SETTING_WHEN_OPEN_1 = 90133;

    /**
     * The error with code <code>90134</code> is thrown when
     * trying to load a Java class that is not part of the allowed classes. By
     * default, all classes are allowed, but this can be changed using the
     * system property h2.allowedClasses.
     */
    public static final int ACCESS_DENIED_TO_CLASS_1 = 90134;

    /**
     * The error with code <code>90135</code> is thrown when
     * trying to open a connection to a database that is currently open
     * in exclusive mode. The exclusive mode is set using:
     * <pre>
     * SET EXCLUSIVE TRUE;
     * </pre>
     */
    public static final int DATABASE_IS_IN_EXCLUSIVE_MODE = 90135;

    /**
     * The error with code <code>90136</code> is thrown when
     * executing a query that used an unsupported outer join condition.
     * Example:
     * <pre>
     * SELECT * FROM DUAL A LEFT JOIN DUAL B ON B.X=(SELECT MAX(X) FROM DUAL);
     * </pre>
     */
    public static final int UNSUPPORTED_OUTER_JOIN_CONDITION_1 = 90136;

    /**
     * The error with code <code>90137</code> is thrown when
     * trying to assign a value to something that is not a variable.
     * <pre>
     * SELECT AMOUNT, SET(@V, IFNULL(@V, 0)+AMOUNT) FROM TEST;
     * </pre>
     */
    public static final int CAN_ONLY_ASSIGN_TO_VARIABLE_1 = 90137;

    /**
     * The error with code <code>90138</code> is thrown when
     *
     * trying to open a persistent database using an incorrect database name.
     * The name of a persistent database contains the path and file name prefix
     * where the data is stored. The file name part of a database name must be
     * at least two characters.
     *
     * Example of wrong usage:
     * <pre>
     * DriverManager.getConnection("jdbc:h2:~/t");
     * DriverManager.getConnection("jdbc:h2:~/test/");
     * </pre>
     * Correct:
     * <pre>
     * DriverManager.getConnection("jdbc:h2:~/te");
     * DriverManager.getConnection("jdbc:h2:~/test/te");
     * </pre>
     */
    public static final int INVALID_DATABASE_NAME_1 = 90138;

    /**
     * The error with code <code>90139</code> is thrown when
     * the specified public static Java method was not found in the class.
     * Example:
     * <pre>
     * CREATE ALIAS TEST FOR "java.lang.Math.test";
     * </pre>
     */
    public static final int PUBLIC_STATIC_JAVA_METHOD_NOT_FOUND_1 = 90139;

    /**
     * The error with code <code>90140</code> is thrown when trying to update or
     * delete a row in a result set if the statement was not created with
     * updatable concurrency. Result sets are only updatable if the statement
     * was created with updatable concurrency, and if the result set contains
     * all columns of the primary key or of a unique index of a table.
     */
    public static final int RESULT_SET_READONLY = 90140;

    /**
     * The error with code <code>90141</code> is thrown when
     * trying to change the java object serializer while there was already data
     * in the database. The serializer of the database must be set when the
     * database is empty.
     */
    public static final int JAVA_OBJECT_SERIALIZER_CHANGE_WITH_DATA_TABLE = 90141;

    /**
     * The error with code <code>90142</code> is thrown when
     * trying to set zero for step size.
     */
    public static final int STEP_SIZE_MUST_NOT_BE_ZERO = 90142;

    /**
     * The error with code <code>90143</code> is thrown when
     * trying to fetch a row from the primary index and the row is not there.
     * Can happen in MULTI_THREADED=1 case.
     */
    public static final int ROW_NOT_FOUND_IN_PRIMARY_INDEX = 90143;

    // next are 90110, 90122, 90144

    private ErrorCode() {
        // utility class
    }

    /**
     * INTERNAL
     */
    public static boolean isCommon(int errorCode) {
        // this list is sorted alphabetically
        switch (errorCode) {
        case DATA_CONVERSION_ERROR_1:
        case DUPLICATE_KEY_1:
        case FUNCTION_ALIAS_ALREADY_EXISTS_1:
        case LOCK_TIMEOUT_1:
        case NULL_NOT_ALLOWED:
        case NO_DATA_AVAILABLE:
        case NUMERIC_VALUE_OUT_OF_RANGE_1:
        case OBJECT_CLOSED:
        case REFERENTIAL_INTEGRITY_VIOLATED_CHILD_EXISTS_1:
        case REFERENTIAL_INTEGRITY_VIOLATED_PARENT_MISSING_1:
        case SYNTAX_ERROR_1:
        case SYNTAX_ERROR_2:
        case TABLE_OR_VIEW_ALREADY_EXISTS_1:
        case TABLE_OR_VIEW_NOT_FOUND_1:
        case VALUE_TOO_LONG_2:
            return true;
        }
        return false;
    }

    /**
     * INTERNAL
     */
    public static String getState(int errorCode) {
        // To convert SQLState to error code, replace
        // 21S: 210, 42S: 421, HY: 50, C: 1, T: 2

        switch (errorCode) {

        // 02: no data
        case NO_DATA_AVAILABLE: return "02000";

        // 07: dynamic SQL error
        case INVALID_PARAMETER_COUNT_2: return "07001";

        // 08: connection exception
        case ERROR_OPENING_DATABASE_1: return "08000";

        // 21: cardinality violation
        case COLUMN_COUNT_DOES_NOT_MATCH: return "21S02";

        // 42: syntax error or access rule violation
        case TABLE_OR_VIEW_ALREADY_EXISTS_1: return "42S01";
        case TABLE_OR_VIEW_NOT_FOUND_1: return "42S02";
        case INDEX_ALREADY_EXISTS_1: return "42S11";
        case INDEX_NOT_FOUND_1: return "42S12";
        case DUPLICATE_COLUMN_NAME_1: return "42S21";
        case COLUMN_NOT_FOUND_1: return "42S22";

        // 0A: feature not supported

        // HZ: remote database access

        // HY
        case GENERAL_ERROR_1: return "HY000";
        case UNKNOWN_DATA_TYPE_1: return "HY004";

        case FEATURE_NOT_SUPPORTED_1: return "HYC00";
        case LOCK_TIMEOUT_1: return "HYT00";
        default:
            return "" + errorCode;
        }
    }

}
