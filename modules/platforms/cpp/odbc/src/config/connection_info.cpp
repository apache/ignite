/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <cstring>
#include <algorithm>

#include "ignite/odbc/system/odbc_constants.h"
#include "ignite/odbc/utility.h"
#include "ignite/odbc/config/connection_info.h"

 // Temporary workaround.
#ifndef SQL_ASYNC_NOTIFICATION
#define SQL_ASYNC_NOTIFICATION                  10025
#endif

#ifndef SQL_ASYNC_NOTIFICATION_NOT_CAPABLE
#define SQL_ASYNC_NOTIFICATION_NOT_CAPABLE      0x00000000L
#endif

#ifndef SQL_ASYNC_NOTIFICATION_CAPABLE
#define SQL_ASYNC_NOTIFICATION_CAPABLE          0x00000001L
#endif

namespace ignite
{
    namespace odbc
    {
        namespace config
        {

#define DBG_STR_CASE(x) case x: return #x

            const char * ConnectionInfo::InfoTypeToString(InfoType type)
            {
                switch (type)
                {
#ifdef SQL_ACCESSIBLE_PROCEDURES
                    DBG_STR_CASE(SQL_ACCESSIBLE_PROCEDURES);
#endif // SQL_ACCESSIBLE_PROCEDURES
#ifdef SQL_ACCESSIBLE_TABLES
                    DBG_STR_CASE(SQL_ACCESSIBLE_TABLES);
#endif // SQL_ACCESSIBLE_TABLES
#ifdef SQL_ACTIVE_ENVIRONMENTS
                    DBG_STR_CASE(SQL_ACTIVE_ENVIRONMENTS);
#endif // SQL_ACTIVE_ENVIRONMENTS
#ifdef SQL_DRIVER_NAME
                    DBG_STR_CASE(SQL_DRIVER_NAME);
#endif // SQL_DRIVER_NAME
#ifdef SQL_DBMS_NAME
                    DBG_STR_CASE(SQL_DBMS_NAME);
#endif // SQL_DBMS_NAME
#ifdef SQL_DRIVER_ODBC_VER
                    DBG_STR_CASE(SQL_DRIVER_ODBC_VER);
#endif // SQL_DRIVER_ODBC_VER
#ifdef SQL_DBMS_VER
                    DBG_STR_CASE(SQL_DBMS_VER);
#endif // SQL_DBMS_VER
#ifdef SQL_DRIVER_VER
                    DBG_STR_CASE(SQL_DRIVER_VER);
#endif // SQL_DRIVER_VER
#ifdef SQL_COLUMN_ALIAS
                    DBG_STR_CASE(SQL_COLUMN_ALIAS);
#endif // SQL_COLUMN_ALIAS
#ifdef SQL_IDENTIFIER_QUOTE_CHAR
                    DBG_STR_CASE(SQL_IDENTIFIER_QUOTE_CHAR);
#endif // SQL_IDENTIFIER_QUOTE_CHAR
#ifdef SQL_CATALOG_NAME_SEPARATOR
                    DBG_STR_CASE(SQL_CATALOG_NAME_SEPARATOR);
#endif // SQL_CATALOG_NAME_SEPARATOR
#ifdef SQL_SPECIAL_CHARACTERS
                    DBG_STR_CASE(SQL_SPECIAL_CHARACTERS);
#endif // SQL_SPECIAL_CHARACTERS
#ifdef SQL_CATALOG_TERM
                    DBG_STR_CASE(SQL_CATALOG_TERM);
#endif // SQL_CATALOG_TERM
#ifdef SQL_TABLE_TERM
                    DBG_STR_CASE(SQL_TABLE_TERM);
#endif // SQL_TABLE_TERM
#ifdef SQL_SCHEMA_TERM
                    DBG_STR_CASE(SQL_SCHEMA_TERM);
#endif // SQL_SCHEMA_TERM
#ifdef SQL_NEED_LONG_DATA_LEN
                    DBG_STR_CASE(SQL_NEED_LONG_DATA_LEN);
#endif // SQL_NEED_LONG_DATA_LEN
#ifdef SQL_ASYNC_DBC_FUNCTIONS
                    DBG_STR_CASE(SQL_ASYNC_DBC_FUNCTIONS);
#endif // SQL_ASYNC_DBC_FUNCTIONS
#ifdef SQL_ASYNC_NOTIFICATION
                    DBG_STR_CASE(SQL_ASYNC_NOTIFICATION);
#endif // SQL_ASYNC_NOTIFICATION
#ifdef SQL_GETDATA_EXTENSIONS
                    DBG_STR_CASE(SQL_GETDATA_EXTENSIONS);
#endif // SQL_GETDATA_EXTENSIONS
#ifdef SQL_ODBC_INTERFACE_CONFORMANCE
                    DBG_STR_CASE(SQL_ODBC_INTERFACE_CONFORMANCE);
#endif // SQL_ODBC_INTERFACE_CONFORMANCE
#ifdef SQL_SQL_CONFORMANCE
                    DBG_STR_CASE(SQL_SQL_CONFORMANCE);
#endif // SQL_SQL_CONFORMANCE
#ifdef SQL_CATALOG_USAGE
                    DBG_STR_CASE(SQL_CATALOG_USAGE);
#endif // SQL_CATALOG_USAGE
#ifdef SQL_SCHEMA_USAGE
                    DBG_STR_CASE(SQL_SCHEMA_USAGE);
#endif // SQL_SCHEMA_USAGE
#ifdef SQL_MAX_IDENTIFIER_LEN
                    DBG_STR_CASE(SQL_MAX_IDENTIFIER_LEN);
#endif // SQL_MAX_IDENTIFIER_LEN
#ifdef SQL_AGGREGATE_FUNCTIONS
                    DBG_STR_CASE(SQL_AGGREGATE_FUNCTIONS);
#endif // SQL_AGGREGATE_FUNCTIONS
#ifdef SQL_NUMERIC_FUNCTIONS
                    DBG_STR_CASE(SQL_NUMERIC_FUNCTIONS);
#endif // SQL_NUMERIC_FUNCTIONS
#ifdef SQL_STRING_FUNCTIONS
                    DBG_STR_CASE(SQL_STRING_FUNCTIONS);
#endif // SQL_STRING_FUNCTIONS
#ifdef SQL_TIMEDATE_FUNCTIONS
                    DBG_STR_CASE(SQL_TIMEDATE_FUNCTIONS);
#endif // SQL_TIMEDATE_FUNCTIONS
#ifdef SQL_TIMEDATE_ADD_INTERVALS
                    DBG_STR_CASE(SQL_TIMEDATE_ADD_INTERVALS);
#endif // SQL_TIMEDATE_ADD_INTERVALS
#ifdef SQL_TIMEDATE_DIFF_INTERVALS
                    DBG_STR_CASE(SQL_TIMEDATE_DIFF_INTERVALS);
#endif // SQL_TIMEDATE_DIFF_INTERVALS
#ifdef SQL_DATETIME_LITERALS
                    DBG_STR_CASE(SQL_DATETIME_LITERALS);
#endif // SQL_DATETIME_LITERALS
#ifdef SQL_SYSTEM_FUNCTIONS
                    DBG_STR_CASE(SQL_SYSTEM_FUNCTIONS);
#endif // SQL_SYSTEM_FUNCTIONS
#ifdef SQL_CONVERT_FUNCTIONS
                    DBG_STR_CASE(SQL_CONVERT_FUNCTIONS);
#endif // SQL_CONVERT_FUNCTIONS
#ifdef SQL_OJ_CAPABILITIES
                    DBG_STR_CASE(SQL_OJ_CAPABILITIES);
#endif // SQL_OJ_CAPABILITIES
#ifdef SQL_POS_OPERATIONS
                    DBG_STR_CASE(SQL_POS_OPERATIONS);
#endif // SQL_POS_OPERATIONS
#ifdef SQL_MAX_CONCURRENT_ACTIVITIES
                    DBG_STR_CASE(SQL_MAX_CONCURRENT_ACTIVITIES);
#endif // SQL_MAX_CONCURRENT_ACTIVITIES
#ifdef SQL_CURSOR_COMMIT_BEHAVIOR
                    DBG_STR_CASE(SQL_CURSOR_COMMIT_BEHAVIOR);
#endif // SQL_CURSOR_COMMIT_BEHAVIOR
#ifdef SQL_CURSOR_ROLLBACK_BEHAVIOR
                    DBG_STR_CASE(SQL_CURSOR_ROLLBACK_BEHAVIOR);
#endif // SQL_CURSOR_ROLLBACK_BEHAVIOR
#ifdef SQL_TXN_CAPABLE
                    DBG_STR_CASE(SQL_TXN_CAPABLE);
#endif // SQL_TXN_CAPABLE
#ifdef SQL_QUOTED_IDENTIFIER_CASE
                    DBG_STR_CASE(SQL_QUOTED_IDENTIFIER_CASE);
#endif // SQL_QUOTED_IDENTIFIER_CASE
#ifdef SQL_SQL92_NUMERIC_VALUE_FUNCTIONS
                    DBG_STR_CASE(SQL_SQL92_NUMERIC_VALUE_FUNCTIONS);
#endif // SQL_SQL92_NUMERIC_VALUE_FUNCTIONS
#ifdef SQL_SQL92_STRING_FUNCTIONS
                    DBG_STR_CASE(SQL_SQL92_STRING_FUNCTIONS);
#endif // SQL_SQL92_STRING_FUNCTIONS
#ifdef SQL_SQL92_DATETIME_FUNCTIONS
                    DBG_STR_CASE(SQL_SQL92_DATETIME_FUNCTIONS);
#endif // SQL_SQL92_DATETIME_FUNCTIONS
#ifdef SQL_SQL92_PREDICATES
                    DBG_STR_CASE(SQL_SQL92_PREDICATES);
#endif // SQL_SQL92_PREDICATES
#ifdef SQL_SQL92_RELATIONAL_JOIN_OPERATORS
                    DBG_STR_CASE(SQL_SQL92_RELATIONAL_JOIN_OPERATORS);
#endif // SQL_SQL92_RELATIONAL_JOIN_OPERATORS
#ifdef SQL_SQL92_VALUE_EXPRESSIONS
                    DBG_STR_CASE(SQL_SQL92_VALUE_EXPRESSIONS);
#endif // SQL_SQL92_VALUE_EXPRESSIONS
#ifdef SQL_STATIC_CURSOR_ATTRIBUTES1
                    DBG_STR_CASE(SQL_STATIC_CURSOR_ATTRIBUTES1);
#endif // SQL_STATIC_CURSOR_ATTRIBUTES1
#ifdef SQL_STATIC_CURSOR_ATTRIBUTES2
                    DBG_STR_CASE(SQL_STATIC_CURSOR_ATTRIBUTES2);
#endif // SQL_STATIC_CURSOR_ATTRIBUTES2
#ifdef SQL_CONVERT_BIGINT
                    DBG_STR_CASE(SQL_CONVERT_BIGINT);
#endif // SQL_CONVERT_BIGINT
#ifdef SQL_CONVERT_BINARY
                    DBG_STR_CASE(SQL_CONVERT_BINARY);
#endif // SQL_CONVERT_BINARY
#ifdef SQL_CONVERT_BIT
                    DBG_STR_CASE(SQL_CONVERT_BIT);
#endif // SQL_CONVERT_BIT
#ifdef SQL_CONVERT_CHAR
                    DBG_STR_CASE(SQL_CONVERT_CHAR);
#endif // SQL_CONVERT_CHAR
#ifdef SQL_CONVERT_DATE
                    DBG_STR_CASE(SQL_CONVERT_DATE);
#endif // SQL_CONVERT_DATE
#ifdef SQL_CONVERT_DECIMAL
                    DBG_STR_CASE(SQL_CONVERT_DECIMAL);
#endif // SQL_CONVERT_DECIMAL
#ifdef SQL_CONVERT_DOUBLE
                    DBG_STR_CASE(SQL_CONVERT_DOUBLE);
#endif // SQL_CONVERT_DOUBLE
#ifdef SQL_CONVERT_FLOAT
                    DBG_STR_CASE(SQL_CONVERT_FLOAT);
#endif // SQL_CONVERT_FLOAT
#ifdef SQL_CONVERT_INTEGER
                    DBG_STR_CASE(SQL_CONVERT_INTEGER);
#endif // SQL_CONVERT_INTEGER
#ifdef SQL_CONVERT_LONGVARCHAR
                    DBG_STR_CASE(SQL_CONVERT_LONGVARCHAR);
#endif // SQL_CONVERT_LONGVARCHAR
#ifdef SQL_CONVERT_NUMERIC
                    DBG_STR_CASE(SQL_CONVERT_NUMERIC);
#endif // SQL_CONVERT_NUMERIC
#ifdef SQL_CONVERT_REAL
                    DBG_STR_CASE(SQL_CONVERT_REAL);
#endif // SQL_CONVERT_REAL
#ifdef SQL_CONVERT_SMALLINT
                    DBG_STR_CASE(SQL_CONVERT_SMALLINT);
#endif // SQL_CONVERT_SMALLINT
#ifdef SQL_CONVERT_TIME
                    DBG_STR_CASE(SQL_CONVERT_TIME);
#endif // SQL_CONVERT_TIME
#ifdef SQL_CONVERT_TIMESTAMP
                    DBG_STR_CASE(SQL_CONVERT_TIMESTAMP);
#endif // SQL_CONVERT_TIMESTAMP
#ifdef SQL_CONVERT_TINYINT
                    DBG_STR_CASE(SQL_CONVERT_TINYINT);
#endif // SQL_CONVERT_TINYINT
#ifdef SQL_CONVERT_VARBINARY
                    DBG_STR_CASE(SQL_CONVERT_VARBINARY);
#endif // SQL_CONVERT_VARBINARY
#ifdef SQL_CONVERT_VARCHAR
                    DBG_STR_CASE(SQL_CONVERT_VARCHAR);
#endif // SQL_CONVERT_VARCHAR
#ifdef SQL_CONVERT_LONGVARBINARY
                    DBG_STR_CASE(SQL_CONVERT_LONGVARBINARY);
#endif // SQL_CONVERT_LONGVARBINARY
#ifdef SQL_CONVERT_WCHAR
                    DBG_STR_CASE(SQL_CONVERT_WCHAR);
#endif // SQL_CONVERT_WCHAR
#ifdef SQL_CONVERT_INTERVAL_DAY_TIME
                    DBG_STR_CASE(SQL_CONVERT_INTERVAL_DAY_TIME);
#endif // SQL_CONVERT_INTERVAL_DAY_TIME
#ifdef SQL_CONVERT_INTERVAL_YEAR_MONTH
                    DBG_STR_CASE(SQL_CONVERT_INTERVAL_YEAR_MONTH);
#endif // SQL_CONVERT_INTERVAL_YEAR_MONTH
#ifdef SQL_CONVERT_WLONGVARCHAR
                    DBG_STR_CASE(SQL_CONVERT_WLONGVARCHAR);
#endif // SQL_CONVERT_WLONGVARCHAR
#ifdef SQL_CONVERT_WVARCHAR
                    DBG_STR_CASE(SQL_CONVERT_WVARCHAR);
#endif // SQL_CONVERT_WVARCHAR
#ifdef SQL_CONVERT_GUID
                    DBG_STR_CASE(SQL_CONVERT_GUID);
#endif // SQL_CONVERT_GUID
#ifdef SQL_SCROLL_OPTIONS
                    DBG_STR_CASE(SQL_SCROLL_OPTIONS);
#endif // SQL_SCROLL_OPTIONS
#ifdef SQL_PARAM_ARRAY_ROW_COUNTS
                    DBG_STR_CASE(SQL_PARAM_ARRAY_ROW_COUNTS);
#endif // SQL_PARAM_ARRAY_ROW_COUNTS
#ifdef SQL_PARAM_ARRAY_SELECTS
                    DBG_STR_CASE(SQL_PARAM_ARRAY_SELECTS);
#endif // SQL_PARAM_ARRAY_SELECTS
#ifdef SQL_ALTER_DOMAIN
                    DBG_STR_CASE(SQL_ALTER_DOMAIN);
#endif // SQL_ALTER_DOMAIN
#ifdef SQL_ASYNC_MODE
                    DBG_STR_CASE(SQL_ASYNC_MODE);
#endif // SQL_ASYNC_MODE
#ifdef SQL_BATCH_ROW_COUNT
                    DBG_STR_CASE(SQL_BATCH_ROW_COUNT);
#endif // SQL_BATCH_ROW_COUNT
#ifdef SQL_BATCH_SUPPORT
                    DBG_STR_CASE(SQL_BATCH_SUPPORT);
#endif // SQL_BATCH_SUPPORT
#ifdef SQL_BOOKMARK_PERSISTENCE
                    DBG_STR_CASE(SQL_BOOKMARK_PERSISTENCE);
#endif // SQL_BOOKMARK_PERSISTENCE
#ifdef SQL_CATALOG_LOCATION
                    DBG_STR_CASE(SQL_CATALOG_LOCATION);
#endif // SQL_CATALOG_LOCATION
#ifdef SQL_CATALOG_NAME
                    DBG_STR_CASE(SQL_CATALOG_NAME);
#endif // SQL_CATALOG_NAME
#ifdef SQL_COLLATION_SEQ
                    DBG_STR_CASE(SQL_COLLATION_SEQ);
#endif // SQL_COLLATION_SEQ
#ifdef SQL_CONCAT_NULL_BEHAVIOR
                    DBG_STR_CASE(SQL_CONCAT_NULL_BEHAVIOR);
#endif // SQL_CONCAT_NULL_BEHAVIOR
#ifdef SQL_CORRELATION_NAME
                    DBG_STR_CASE(SQL_CORRELATION_NAME);
#endif // SQL_CORRELATION_NAME
#ifdef SQL_CREATE_ASSERTION
                    DBG_STR_CASE(SQL_CREATE_ASSERTION);
#endif // SQL_CREATE_ASSERTION
#ifdef SQL_CREATE_CHARACTER_SET
                    DBG_STR_CASE(SQL_CREATE_CHARACTER_SET);
#endif // SQL_CREATE_CHARACTER_SET
#ifdef SQL_CREATE_COLLATION
                    DBG_STR_CASE(SQL_CREATE_COLLATION);
#endif // SQL_CREATE_COLLATION
#ifdef SQL_CREATE_DOMAIN
                    DBG_STR_CASE(SQL_CREATE_DOMAIN);
#endif // SQL_CREATE_DOMAIN
#ifdef SQL_CREATE_TABLE
                    DBG_STR_CASE(SQL_CREATE_TABLE);
#endif // SQL_CREATE_TABLE
#ifdef SQL_CREATE_TRANSLATION
                    DBG_STR_CASE(SQL_CREATE_TRANSLATION);
#endif // SQL_CREATE_TRANSLATION
#ifdef SQL_CREATE_VIEW
                    DBG_STR_CASE(SQL_CREATE_VIEW);
#endif // SQL_CREATE_VIEW
#ifdef SQL_CURSOR_SENSITIVITY
                    DBG_STR_CASE(SQL_CURSOR_SENSITIVITY);
#endif // SQL_CURSOR_SENSITIVITY
#ifdef SQL_DATA_SOURCE_NAME
                    DBG_STR_CASE(SQL_DATA_SOURCE_NAME);
#endif // SQL_DATA_SOURCE_NAME
#ifdef SQL_DATA_SOURCE_READ_ONLY
                    DBG_STR_CASE(SQL_DATA_SOURCE_READ_ONLY);
#endif // SQL_DATA_SOURCE_READ_ONLY
#ifdef SQL_DATABASE_NAME
                    DBG_STR_CASE(SQL_DATABASE_NAME);
#endif // SQL_DATABASE_NAME
#ifdef SQL_DDL_INDEX
                    DBG_STR_CASE(SQL_DDL_INDEX);
#endif // SQL_DDL_INDEX
#ifdef SQL_DEFAULT_TXN_ISOLATION
                    DBG_STR_CASE(SQL_DEFAULT_TXN_ISOLATION);
#endif // SQL_DEFAULT_TXN_ISOLATION
#ifdef SQL_DESCRIBE_PARAMETER
                    DBG_STR_CASE(SQL_DESCRIBE_PARAMETER);
#endif // SQL_DESCRIBE_PARAMETER
#ifdef SQL_DROP_ASSERTION
                    DBG_STR_CASE(SQL_DROP_ASSERTION);
#endif // SQL_DROP_ASSERTION
#ifdef SQL_DROP_CHARACTER_SET
                    DBG_STR_CASE(SQL_DROP_CHARACTER_SET);
#endif // SQL_DROP_CHARACTER_SET
#ifdef SQL_DROP_COLLATION
                    DBG_STR_CASE(SQL_DROP_COLLATION);
#endif // SQL_DROP_COLLATION
#ifdef SQL_DROP_DOMAIN
                    DBG_STR_CASE(SQL_DROP_DOMAIN);
#endif // SQL_DROP_DOMAIN
#ifdef SQL_DROP_SCHEMA
                    DBG_STR_CASE(SQL_DROP_SCHEMA);
#endif // SQL_DROP_SCHEMA
#ifdef SQL_DROP_TABLE
                    DBG_STR_CASE(SQL_DROP_TABLE);
#endif // SQL_DROP_TABLE
#ifdef SQL_DROP_TRANSLATION
                    DBG_STR_CASE(SQL_DROP_TRANSLATION);
#endif // SQL_DROP_TRANSLATION
#ifdef SQL_DROP_VIEW
                    DBG_STR_CASE(SQL_DROP_VIEW);
#endif // SQL_DROP_VIEW
#ifdef SQL_DYNAMIC_CURSOR_ATTRIBUTES1
                    DBG_STR_CASE(SQL_DYNAMIC_CURSOR_ATTRIBUTES1);
#endif // SQL_DYNAMIC_CURSOR_ATTRIBUTES1
#ifdef SQL_DYNAMIC_CURSOR_ATTRIBUTES2
                    DBG_STR_CASE(SQL_DYNAMIC_CURSOR_ATTRIBUTES2);
#endif // SQL_DYNAMIC_CURSOR_ATTRIBUTES2
#ifdef SQL_EXPRESSIONS_IN_ORDERBY
                    DBG_STR_CASE(SQL_EXPRESSIONS_IN_ORDERBY);
#endif // SQL_EXPRESSIONS_IN_ORDERBY
#ifdef SQL_FILE_USAGE
                    DBG_STR_CASE(SQL_FILE_USAGE);
#endif // SQL_FILE_USAGE
#ifdef SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1
                    DBG_STR_CASE(SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1);
#endif // SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1
#ifdef SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2
                    DBG_STR_CASE(SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2);
#endif // SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2
#ifdef SQL_GROUP_BY
                    DBG_STR_CASE(SQL_GROUP_BY);
#endif // SQL_GROUP_BY
#ifdef SQL_IDENTIFIER_CASE
                    DBG_STR_CASE(SQL_IDENTIFIER_CASE);
#endif // SQL_IDENTIFIER_CASE
#ifdef SQL_INDEX_KEYWORDS
                    DBG_STR_CASE(SQL_INDEX_KEYWORDS);
#endif // SQL_INDEX_KEYWORDS
#ifdef SQL_INFO_SCHEMA_VIEWS
                    DBG_STR_CASE(SQL_INFO_SCHEMA_VIEWS);
#endif // SQL_INFO_SCHEMA_VIEWS
#ifdef SQL_INSERT_STATEMENT
                    DBG_STR_CASE(SQL_INSERT_STATEMENT);
#endif // SQL_INSERT_STATEMENT
#ifdef SQL_INTEGRITY
                    DBG_STR_CASE(SQL_INTEGRITY);
#endif // SQL_INTEGRITY
#ifdef SQL_KEYSET_CURSOR_ATTRIBUTES1
                    DBG_STR_CASE(SQL_KEYSET_CURSOR_ATTRIBUTES1);
#endif // SQL_KEYSET_CURSOR_ATTRIBUTES1
#ifdef SQL_KEYSET_CURSOR_ATTRIBUTES2
                    DBG_STR_CASE(SQL_KEYSET_CURSOR_ATTRIBUTES2);
#endif // SQL_KEYSET_CURSOR_ATTRIBUTES2
#ifdef SQL_KEYWORDS
                    DBG_STR_CASE(SQL_KEYWORDS);
#endif // SQL_KEYWORDS
#ifdef SQL_LIKE_ESCAPE_CLAUSE
                    DBG_STR_CASE(SQL_LIKE_ESCAPE_CLAUSE);
#endif // SQL_LIKE_ESCAPE_CLAUSE
#ifdef SQL_MAX_ASYNC_CONCURRENT_STATEMENTS
                    DBG_STR_CASE(SQL_MAX_ASYNC_CONCURRENT_STATEMENTS);
#endif // SQL_MAX_ASYNC_CONCURRENT_STATEMENTS
#ifdef SQL_MAX_BINARY_LITERAL_LEN
                    DBG_STR_CASE(SQL_MAX_BINARY_LITERAL_LEN);
#endif // SQL_MAX_BINARY_LITERAL_LEN
#ifdef SQL_MAX_CATALOG_NAME_LEN
                    DBG_STR_CASE(SQL_MAX_CATALOG_NAME_LEN);
#endif // SQL_MAX_CATALOG_NAME_LEN
#ifdef SQL_MAX_CHAR_LITERAL_LEN
                    DBG_STR_CASE(SQL_MAX_CHAR_LITERAL_LEN);
#endif // SQL_MAX_CHAR_LITERAL_LEN
#ifdef SQL_MAX_COLUMN_NAME_LEN
                    DBG_STR_CASE(SQL_MAX_COLUMN_NAME_LEN);
#endif // SQL_MAX_COLUMN_NAME_LEN
#ifdef SQL_MAX_COLUMNS_IN_GROUP_BY
                    DBG_STR_CASE(SQL_MAX_COLUMNS_IN_GROUP_BY);
#endif // SQL_MAX_COLUMNS_IN_GROUP_BY
#ifdef SQL_MAX_COLUMNS_IN_INDEX
                    DBG_STR_CASE(SQL_MAX_COLUMNS_IN_INDEX);
#endif // SQL_MAX_COLUMNS_IN_INDEX
#ifdef SQL_MAX_COLUMNS_IN_ORDER_BY
                    DBG_STR_CASE(SQL_MAX_COLUMNS_IN_ORDER_BY);
#endif // SQL_MAX_COLUMNS_IN_ORDER_BY
#ifdef SQL_MAX_COLUMNS_IN_SELECT
                    DBG_STR_CASE(SQL_MAX_COLUMNS_IN_SELECT);
#endif // SQL_MAX_COLUMNS_IN_SELECT
#ifdef SQL_MAX_COLUMNS_IN_TABLE
                    DBG_STR_CASE(SQL_MAX_COLUMNS_IN_TABLE);
#endif // SQL_MAX_COLUMNS_IN_TABLE
#ifdef SQL_MAX_CURSOR_NAME_LEN
                    DBG_STR_CASE(SQL_MAX_CURSOR_NAME_LEN);
#endif // SQL_MAX_CURSOR_NAME_LEN
#ifdef SQL_MAX_DRIVER_CONNECTIONS
                    DBG_STR_CASE(SQL_MAX_DRIVER_CONNECTIONS);
#endif // SQL_MAX_DRIVER_CONNECTIONS
#ifdef SQL_MAX_INDEX_SIZE
                    DBG_STR_CASE(SQL_MAX_INDEX_SIZE);
#endif // SQL_MAX_INDEX_SIZE
#ifdef SQL_MAX_PROCEDURE_NAME_LEN
                    DBG_STR_CASE(SQL_MAX_PROCEDURE_NAME_LEN);
#endif // SQL_MAX_PROCEDURE_NAME_LEN
#ifdef SQL_MAX_ROW_SIZE
                    DBG_STR_CASE(SQL_MAX_ROW_SIZE);
#endif // SQL_MAX_ROW_SIZE
#ifdef SQL_MAX_ROW_SIZE_INCLUDES_LONG
                    DBG_STR_CASE(SQL_MAX_ROW_SIZE_INCLUDES_LONG);
#endif // SQL_MAX_ROW_SIZE_INCLUDES_LONG
#ifdef SQL_MAX_SCHEMA_NAME_LEN
                    DBG_STR_CASE(SQL_MAX_SCHEMA_NAME_LEN);
#endif // SQL_MAX_SCHEMA_NAME_LEN
#ifdef SQL_MAX_STATEMENT_LEN
                    DBG_STR_CASE(SQL_MAX_STATEMENT_LEN);
#endif // SQL_MAX_STATEMENT_LEN
#ifdef SQL_MAX_TABLE_NAME_LEN
                    DBG_STR_CASE(SQL_MAX_TABLE_NAME_LEN);
#endif // SQL_MAX_TABLE_NAME_LEN
#ifdef SQL_MAX_TABLES_IN_SELECT
                    DBG_STR_CASE(SQL_MAX_TABLES_IN_SELECT);
#endif // SQL_MAX_TABLES_IN_SELECT
#ifdef SQL_MAX_USER_NAME_LEN
                    DBG_STR_CASE(SQL_MAX_USER_NAME_LEN);
#endif // SQL_MAX_USER_NAME_LEN
#ifdef SQL_MULT_RESULT_SETS
                    DBG_STR_CASE(SQL_MULT_RESULT_SETS);
#endif // SQL_MULT_RESULT_SETS
#ifdef SQL_MULTIPLE_ACTIVE_TXN
                    DBG_STR_CASE(SQL_MULTIPLE_ACTIVE_TXN);
#endif // SQL_MULTIPLE_ACTIVE_TXN
#ifdef SQL_NON_NULLABLE_COLUMNS
                    DBG_STR_CASE(SQL_NON_NULLABLE_COLUMNS);
#endif // SQL_NON_NULLABLE_COLUMNS
#ifdef SQL_NULL_COLLATION
                    DBG_STR_CASE(SQL_NULL_COLLATION);
#endif // SQL_NULL_COLLATION
#ifdef SQL_ORDER_BY_COLUMNS_IN_SELECT
                    DBG_STR_CASE(SQL_ORDER_BY_COLUMNS_IN_SELECT);
#endif // SQL_ORDER_BY_COLUMNS_IN_SELECT
#ifdef SQL_PROCEDURE_TERM
                    DBG_STR_CASE(SQL_PROCEDURE_TERM);
#endif // SQL_PROCEDURE_TERM
#ifdef SQL_PROCEDURES
                    DBG_STR_CASE(SQL_PROCEDURES);
#endif // SQL_PROCEDURES
#ifdef SQL_ROW_UPDATES
                    DBG_STR_CASE(SQL_ROW_UPDATES);
#endif // SQL_ROW_UPDATES
#ifdef SQL_SEARCH_PATTERN_ESCAPE
                    DBG_STR_CASE(SQL_SEARCH_PATTERN_ESCAPE);
#endif // SQL_SEARCH_PATTERN_ESCAPE
#ifdef SQL_SERVER_NAME
                    DBG_STR_CASE(SQL_SERVER_NAME);
#endif // SQL_SERVER_NAME
#ifdef SQL_SQL92_FOREIGN_KEY_DELETE_RULE
                    DBG_STR_CASE(SQL_SQL92_FOREIGN_KEY_DELETE_RULE);
#endif // SQL_SQL92_FOREIGN_KEY_DELETE_RULE
#ifdef SQL_SQL92_FOREIGN_KEY_UPDATE_RULE
                    DBG_STR_CASE(SQL_SQL92_FOREIGN_KEY_UPDATE_RULE);
#endif // SQL_SQL92_FOREIGN_KEY_UPDATE_RULE
#ifdef SQL_SQL92_GRANT
                    DBG_STR_CASE(SQL_SQL92_GRANT);
#endif // SQL_SQL92_GRANT
#ifdef SQL_SQL92_REVOKE
                    DBG_STR_CASE(SQL_SQL92_REVOKE);
#endif // SQL_SQL92_REVOKE
#ifdef SQL_SQL92_ROW_VALUE_CONSTRUCTOR
                    DBG_STR_CASE(SQL_SQL92_ROW_VALUE_CONSTRUCTOR);
#endif // SQL_SQL92_ROW_VALUE_CONSTRUCTOR
#ifdef SQL_STANDARD_CLI_CONFORMANCE
                    DBG_STR_CASE(SQL_STANDARD_CLI_CONFORMANCE);
#endif // SQL_STANDARD_CLI_CONFORMANCE
#ifdef SQL_SUBQUERIES
                    DBG_STR_CASE(SQL_SUBQUERIES);
#endif // SQL_SUBQUERIES
#ifdef SQL_TXN_ISOLATION_OPTION
                    DBG_STR_CASE(SQL_TXN_ISOLATION_OPTION);
#endif // SQL_TXN_ISOLATION_OPTION
#ifdef SQL_UNION
                    DBG_STR_CASE(SQL_UNION);
#endif // SQL_UNION
#ifdef SQL_USER_NAME
                    DBG_STR_CASE(SQL_USER_NAME);
#endif // SQL_USER_NAME
#ifdef SQL_ALTER_TABLE
                    DBG_STR_CASE(SQL_ALTER_TABLE);
#endif // SQL_ALTER_TABLE
#ifdef SQL_FETCH_DIRECTION
                    DBG_STR_CASE(SQL_FETCH_DIRECTION);
#endif // SQL_FETCH_DIRECTION
#ifdef SQL_LOCK_TYPES
                    DBG_STR_CASE(SQL_LOCK_TYPES);
#endif // SQL_LOCK_TYPES
#ifdef SQL_ODBC_API_CONFORMANCE
                    DBG_STR_CASE(SQL_ODBC_API_CONFORMANCE);
#endif // SQL_ODBC_API_CONFORMANCE
#ifdef SQL_ODBC_SQL_CONFORMANCE
                    DBG_STR_CASE(SQL_ODBC_SQL_CONFORMANCE);
#endif // SQL_ODBC_SQL_CONFORMANCE
#ifdef SQL_POSITIONED_STATEMENTS
                    DBG_STR_CASE(SQL_POSITIONED_STATEMENTS);
#endif // SQL_POSITIONED_STATEMENTS
#ifdef SQL_SCROLL_CONCURRENCY
                    DBG_STR_CASE(SQL_SCROLL_CONCURRENCY);
#endif // SQL_SCROLL_CONCURRENCY
#ifdef SQL_STATIC_SENSITIVITY
                    DBG_STR_CASE(SQL_STATIC_SENSITIVITY);
#endif // SQL_STATIC_SENSITIVITY
#ifdef SQL_DTC_TRANSITION_COST
                    DBG_STR_CASE(SQL_DTC_TRANSITION_COST);
#endif // SQL_DTC_TRANSITION_COST
                    default:
                        break;
                }
                return "<< UNKNOWN TYPE >>";
            }

#undef DBG_STR_CASE

            ConnectionInfo::ConnectionInfo(const Configuration& config) :
                strParams(),
                intParams(),
                shortParams(),
                config(config)
            {
                //
                //======================= String Params =======================
                //

                // Driver name.
#ifdef SQL_DRIVER_NAME
                strParams[SQL_DRIVER_NAME] = "Apache Ignite";
#endif // SQL_DRIVER_NAME
#ifdef SQL_DBMS_NAME
                strParams[SQL_DBMS_NAME]   = "Apache Ignite";
#endif // SQL_DBMS_NAME

                // ODBC version.
#ifdef SQL_DRIVER_ODBC_VER
                strParams[SQL_DRIVER_ODBC_VER] = "03.00";
#endif // SQL_DRIVER_ODBC_VER

#ifdef SQL_DRIVER_VER
                // Driver version. At a minimum, the version is of the form ##.##.####, where the first two digits are
                // the major version, the next two digits are the minor version, and the last four digits are the
                // release version.
                strParams[SQL_DRIVER_VER] = "02.04.0000";
#endif // SQL_DRIVER_VER
#ifdef SQL_DBMS_VER
                strParams[SQL_DBMS_VER] = "02.04.0000";
#endif // SQL_DBMS_VER

#ifdef SQL_COLUMN_ALIAS
                // A character string: "Y" if the data source supports column aliases; otherwise, "N".
                strParams[SQL_COLUMN_ALIAS] = "Y";
#endif // SQL_COLUMN_ALIAS

#ifdef SQL_IDENTIFIER_QUOTE_CHAR
                // The character string that is used as the starting and ending delimiter of a quoted (delimited)
                // identifier in SQL statements. Identifiers passed as arguments to ODBC functions do not have to be
                // quoted. If the data source does not support quoted identifiers, a blank is returned.
                strParams[SQL_IDENTIFIER_QUOTE_CHAR] = "";
#endif // SQL_IDENTIFIER_QUOTE_CHAR

#ifdef SQL_CATALOG_NAME_SEPARATOR
                // A character string: the character or characters that the data source defines as the separator between
                // a catalog name and the qualified name element that follows or precedes it.
                strParams[SQL_CATALOG_NAME_SEPARATOR] = ".";
#endif // SQL_CATALOG_NAME_SEPARATOR

#ifdef SQL_SPECIAL_CHARACTERS
                // A character string that contains all special characters (that is, all characters except a through z,
                // A through Z, 0 through 9, and underscore) that can be used in an identifier name, such as a table
                // name, column name, or index name, on the data source.
                strParams[SQL_SPECIAL_CHARACTERS] = "";
#endif // SQL_SPECIAL_CHARACTERS

#ifdef SQL_CATALOG_TERM
                // A character string with the data source vendor's name for a catalog; for example, "database" or
                // "directory". This string can be in upper, lower, or mixed case. This InfoType has been renamed for
                // ODBC 3.0 from the ODBC 2.0 InfoType SQL_QUALIFIER_TERM.
                strParams[SQL_CATALOG_TERM] = "";
#endif // SQL_CATALOG_TERM

#ifdef SQL_QUALIFIER_TERM
                strParams[SQL_QUALIFIER_TERM] = "";
#endif // SQL_QUALIFIER_TERM

#ifdef SQL_TABLE_TERM
                // A character string with the data source vendor's name for a table; for example, "table" or "file".
                strParams[SQL_TABLE_TERM] = "table";
#endif // SQL_TABLE_TERM

#ifdef SQL_SCHEMA_TERM
                // A character string with the data source vendor's name for a schema; for example, "owner",
                // "Authorization ID", or "Schema".
                strParams[SQL_SCHEMA_TERM] = "schema";
#endif // SQL_SCHEMA_TERM

#ifdef SQL_NEED_LONG_DATA_LEN
                // A character string: "Y" if the data source needs the length of a long data value (the data type is
                // SQL_LONGVARCHAR, SQL_LONGVARBINARY) before that value is sent to the data source, "N" if it does not.
                strParams[SQL_NEED_LONG_DATA_LEN ] = "Y";
#endif // SQL_NEED_LONG_DATA_LEN

#ifdef SQL_ACCESSIBLE_PROCEDURES
                // A character string: "Y" if the user can execute all procedures returned by SQLProcedures; "N" if
                // there may be procedures returned that the user cannot execute.
                strParams[SQL_ACCESSIBLE_PROCEDURES] = "Y";
#endif // SQL_ACCESSIBLE_PROCEDURES

#ifdef SQL_ACCESSIBLE_TABLES
                // A character string: "Y" if the user is guaranteed SELECT privileges to all tables returned by
                // SQLTables; "N" if there may be tables returned that the user cannot access.
                strParams[SQL_ACCESSIBLE_TABLES] = "Y";
#endif // SQL_ACCESSIBLE_TABLES

#ifdef SQL_CATALOG_NAME
                // A character string: "Y" if the server supports catalog names, or "N" if it does not.
                // An SQL - 92 Full level-conformant driver will always return "Y".
                strParams[SQL_CATALOG_NAME] = "N";
#endif // SQL_CATALOG_NAME

#ifdef SQL_COLLATION_SEQ
                // The name of the collation sequence. This is a character string that indicates the name of the default
                // collation for the default character set for this server (for example, 'ISO 8859-1' or EBCDIC). If
                // this is unknown, an empty string will be returned. An SQL-92 Full level-conformant driver will always
                // return a non-empty string.
                strParams[SQL_COLLATION_SEQ] = "UTF-8";
#endif // SQL_COLLATION_SEQ

#ifdef SQL_DATA_SOURCE_NAME
                // A character string with the data source name that was used during connection.
                //
                // If the application called SQLConnect, this is the value of the szDSN argument. If the application
                // called SQLDriverConnect or SQLBrowseConnect, this is the value of the DSN keyword in the connection
                // string passed to the driver. If the connection string did not contain the DSN keyword (such as when
                // it contains the DRIVER keyword), this is an empty string.
                strParams[SQL_DATA_SOURCE_NAME] = config.GetDsn("");
#endif // SQL_DATA_SOURCE_NAME

#ifdef SQL_DATA_SOURCE_READ_ONLY
                // A character string. "Y" if the data source is set to READ ONLY mode, "N" if it is otherwise.
                //
                // This characteristic pertains only to the data source itself; it is not a characteristic of the driver
                // that enables access to the data source. A driver that is read/write can be used with a data source
                // that is read-only. If a driver is read-only, all of its data sources must be read-only and must
                // return SQL_DATA_SOURCE_READ_ONLY.
                strParams[SQL_DATA_SOURCE_READ_ONLY] = "N";
#endif // SQL_DATA_SOURCE_READ_ONLY

#ifdef SQL_DATABASE_NAME
                // A character string with the name of the current database in use, if the data source defines a named
                // object called "database".
                strParams[SQL_DATABASE_NAME] = "";
#endif // SQL_DATABASE_NAME

#ifdef SQL_DESCRIBE_PARAMETER
                // A character string: "Y" if parameters can be described; "N", if not.
                // An SQL-92 Full level-conformant driver will usually return "Y" because it will support the DESCRIBE
                // INPUT statement. Because this does not directly specify the underlying SQL support, however,
                // describing parameters might not be supported, even in a SQL-92 Full level-conformant driver.
                strParams[SQL_DESCRIBE_PARAMETER] = "N";
#endif // SQL_DESCRIBE_PARAMETER

#ifdef SQL_EXPRESSIONS_IN_ORDERBY
                // A character string: "Y" if the data source supports expressions in the ORDER BY list; "N" if it does
                // not.
                strParams[SQL_EXPRESSIONS_IN_ORDERBY] = "Y";
#endif // SQL_EXPRESSIONS_IN_ORDERBY

#ifdef SQL_INTEGRITY
                // A character string: "Y" if the data source supports the Integrity Enhancement Facility; "N" if it
                // does not.
                strParams[SQL_INTEGRITY] = "N";
#endif // SQL_INTEGRITY

#ifdef SQL_KEYWORDS
                // A character string that contains a comma-separated list of all data source-specific keywords. This
                // list does not contain keywords specific to ODBC or keywords used by both the data source and ODBC.
                // This list represents all the reserved keywords; interoperable applications should not use these words
                // in object names.
                // The #define value SQL_ODBC_KEYWORDS contains a comma - separated list of ODBC keywords.
                strParams[SQL_KEYWORDS] = "LIMIT,MINUS,OFFSET,ROWNUM,SYSDATE,SYSTIME,SYSTIMESTAMP,TODAY";
#endif // SQL_KEYWORDS

#ifdef SQL_LIKE_ESCAPE_CLAUSE
                // A character string: "Y" if the data source supports an escape character for the percent character (%)
                // and underscore character (_) in a LIKE predicate and the driver supports the ODBC syntax for defining
                // a LIKE predicate escape character; "N" otherwise.
                strParams[SQL_LIKE_ESCAPE_CLAUSE] = "N";
#endif // SQL_LIKE_ESCAPE_CLAUSE

#ifdef SQL_MAX_ROW_SIZE_INCLUDES_LONG
                // A character string: "Y" if the maximum row size returned for the SQL_MAX_ROW_SIZE information type
                // includes the length of all SQL_LONGVARCHAR and SQL_LONGVARBINARY columns in the row; "N" otherwise.
                strParams[SQL_MAX_ROW_SIZE_INCLUDES_LONG] = "Y";
#endif // SQL_MAX_ROW_SIZE_INCLUDES_LONG

#ifdef SQL_MULT_RESULT_SETS
                // A character string: "Y" if the data source supports multiple result sets, "N" if it does not.
                strParams[SQL_MULT_RESULT_SETS] = "N";
#endif // SQL_MULT_RESULT_SETS

#ifdef SQL_MULTIPLE_ACTIVE_TXN
                // A character string: "Y" if the driver supports more than one active transaction at the same time,
                // "N" if only one transaction can be active at any time.
                strParams[SQL_MULTIPLE_ACTIVE_TXN] = "Y";
#endif // SQL_MULTIPLE_ACTIVE_TXN

#ifdef SQL_ORDER_BY_COLUMNS_IN_SELECT
                // A character string: "Y" if the columns in the ORDER BY clause must be in the select list;
                // otherwise, "N".
                strParams[SQL_ORDER_BY_COLUMNS_IN_SELECT] = "N";
#endif // SQL_ORDER_BY_COLUMNS_IN_SELECT

#ifdef SQL_PROCEDURE_TERM
                // A character string with the data source vendor's name for a procedure; for example,
                // "database procedure", "stored procedure", "procedure", "package", or "stored query".
                strParams[SQL_PROCEDURE_TERM] = "stored procedure";
#endif // SQL_PROCEDURE_TERM

#ifdef SQL_PROCEDURE_TERM
                // A character string: "Y" if the data source supports procedures and the driver supports the ODBC
                // procedure invocation syntax; "N" otherwise.
                strParams[SQL_PROCEDURES] = "N";
#endif // SQL_PROCEDURE_TERM

#ifdef SQL_ROW_UPDATES
                // A character string: "Y" if a keyset-driven or mixed cursor maintains row versions or values for all
                // fetched rows and therefore can detect any updates that were made to a row by any user since the row
                // was last fetched. (This applies only to updates, not to deletions or insertions.) The driver can
                // return the SQL_ROW_UPDATED flag to the row status array when SQLFetchScroll is called. Otherwise, "N"
                strParams[SQL_ROW_UPDATES] = "N";
#endif // SQL_ROW_UPDATES

#ifdef SQL_SEARCH_PATTERN_ESCAPE
                // A character string specifying what the driver supports as an escape character that allows the use of
                // the pattern match metacharacters underscore (_) and percent sign (%) as valid characters in search
                // patterns. This escape character applies only for those catalog function arguments that support search
                // strings. If this string is empty, the driver does not support a search-pattern escape character.
                // Because this information type does not indicate general support of the escape character in the LIKE
                // predicate, SQL-92 does not include requirements for this character string.
                // This InfoType is limited to catalog functions. For a description of the use of the escape character
                // in search pattern strings, see Pattern Value Arguments.
                strParams[SQL_SEARCH_PATTERN_ESCAPE] = "\\";
#endif // SQL_SEARCH_PATTERN_ESCAPE

#ifdef SQL_SERVER_NAME
                // A character string with the actual data source-specific server name; useful when a data source name
                // is used during SQLConnect, SQLDriverConnect, and SQLBrowseConnect.
                strParams[SQL_SERVER_NAME] = "Apache Ignite";
#endif // SQL_SERVER_NAME

#ifdef SQL_USER_NAME
                // A character string with the name used in a particular database, which can be different from the login
                // name.
                strParams[SQL_USER_NAME] = "apache_ignite_user";
#endif // SQL_USER_NAME

                //
                //====================== Integer Params =======================
                //

#ifdef SQL_ASYNC_DBC_FUNCTIONS
                // Indicates if the driver can execute functions asynchronously on the connection handle.
                // SQL_ASYNC_DBC_CAPABLE = The driver can execute connection functions asynchronously.
                // SQL_ASYNC_DBC_NOT_CAPABLE = The driver can not execute connection functions asynchronously.
                intParams[SQL_ASYNC_DBC_FUNCTIONS] = SQL_ASYNC_DBC_NOT_CAPABLE;
#endif // SQL_ASYNC_DBC_FUNCTIONS

#ifdef SQL_ASYNC_MODE
                // Indicates the level of asynchronous support in the driver:
                // SQL_AM_CONNECTION = Connection level asynchronous execution is supported.Either all statement handles
                //    associated with a given connection handle are in asynchronous mode or all are in synchronous mode.
                //    A statement handle on a connection cannot be in asynchronous mode while another statement handle
                //    on the same connection is in synchronous mode, and vice versa.
                // SQL_AM_STATEMENT = Statement level asynchronous execution is supported.Some statement handles
                //    associated with a connection handle can be in asynchronous mode, while other statement handles on
                //    the same connection are in synchronous mode.
                // SQL_AM_NONE = Asynchronous mode is not supported.
                intParams[SQL_ASYNC_MODE] = SQL_AM_NONE;
#endif // SQL_ASYNC_MODE

#ifdef SQL_ASYNC_NOTIFICATION
                // Indicates if the driver supports asynchronous notification:
                // SQL_ASYNC_NOTIFICATION_CAPABLE Asynchronous execution notification is supported by the driver.
                // SQL_ASYNC_NOTIFICATION_NOT_CAPABLE Asynchronous execution notification is not supported by the
                //     driver.
                //
                // There are two categories of ODBC asynchronous operations: connection level asynchronous operations
                // and statement level asynchronous operations. If a driver returns SQL_ASYNC_NOTIFICATION_CAPABLE, it
                // must support notification for all APIs that it can execute asynchronously.
                intParams[SQL_ASYNC_NOTIFICATION] = SQL_ASYNC_NOTIFICATION_NOT_CAPABLE;
#endif // SQL_ASYNC_NOTIFICATION

#ifdef SQL_BATCH_ROW_COUNT
                // Enumerates the behavior of the driver with respect to the availability of row counts. The following
                // bitmasks are used together with the information type:
                // SQL_BRC_ROLLED_UP = Row counts for consecutive INSERT, DELETE, or UPDATE statements are rolled up
                //     into one. If this bit is not set, row counts are available for each statement.
                // SQL_BRC_PROCEDURES = Row counts, if any, are available when a batch is executed in a stored
                //     procedure. If row counts are available, they can be rolled up or individually available,
                //     depending on the SQL_BRC_ROLLED_UP bit.
                // SQL_BRC_EXPLICIT = Row counts, if any, are available when a batch is executed directly by calling
                //     SQLExecute or SQLExecDirect. If row counts are available, they can be rolled up or individually
                //     available, depending on the SQL_BRC_ROLLED_UP bit.
                intParams[SQL_BATCH_ROW_COUNT] = SQL_BRC_ROLLED_UP | SQL_BRC_EXPLICIT;
#endif // SQL_BATCH_ROW_COUNT

#ifdef SQL_BATCH_SUPPORT
                // Bitmask enumerating the driver's support for batches. The following bitmasks are used to determine
                // which level is supported:
                // SQL_BS_SELECT_EXPLICIT = The driver supports explicit batches that can have result - set generating
                //     statements.
                // SQL_BS_ROW_COUNT_EXPLICIT = The driver supports explicit batches that can have row - count generating
                //     statements.
                // SQL_BS_SELECT_PROC = The driver supports explicit procedures that can have result - set generating
                //     statements.
                // SQL_BS_ROW_COUNT_PROC = The driver supports explicit procedures that can have row - count generating
                //     statements.
                intParams[SQL_BATCH_SUPPORT] = SQL_BS_ROW_COUNT_EXPLICIT;
#endif // SQL_BATCH_SUPPORT

#ifdef SQL_BOOKMARK_PERSISTENCE
                // Bitmask enumerating the operations through which bookmarks persist. The following bitmasks are used
                // together with the flag to determine through which options bookmarks persist:
                // SQL_BP_CLOSE = Bookmarks are valid after an application calls SQLFreeStmt with the SQL_CLOSE option,
                //     or SQLCloseCursor to close the cursor associated with a statement.
                // SQL_BP_DELETE = The bookmark for a row is valid after that row has been deleted.
                // SQL_BP_DROP = Bookmarks are valid after an application calls SQLFreeHandle with a HandleType of
                //     SQL_HANDLE_STMT to drop a statement.
                // SQL_BP_TRANSACTION = Bookmarks are valid after an application commits or rolls back a transaction.
                // SQL_BP_UPDATE = The bookmark for a row is valid after any column in that row has been updated,
                //     including key columns.
                // SQL_BP_OTHER_HSTMT = A bookmark associated with one statement can be used with another statement.
                //     Unless SQL_BP_CLOSE or SQL_BP_DROP is specified, the cursor on the first statement must be open.
                intParams[SQL_BOOKMARK_PERSISTENCE] = 0;
#endif // SQL_BOOKMARK_PERSISTENCE

#ifdef SQL_CATALOG_LOCATION
                // Value that indicates the position of the catalog in a qualified table name: SQL_CL_START, SQL_CL_END
                //
                // An SQL - 92 Full level-conformant driver will always return SQL_CL_START.A value of 0 is returned if
                // catalogs are not supported by the data source. This InfoType has been renamed for ODBC 3.0 from the
                // ODBC 2.0 InfoType SQL_QUALIFIER_LOCATION.
                intParams[SQL_CATALOG_LOCATION] = 0;
#endif // SQL_CATALOG_LOCATION

#ifdef SQL_QUALIFIER_LOCATION
                intParams[SQL_QUALIFIER_LOCATION] = 0;
#endif // SQL_QUALIFIER_LOCATION

#ifdef SQL_GETDATA_EXTENSIONS
                // Bitmask enumerating extensions to SQLGetData.
                // SQL_GD_ANY_COLUMN = SQLGetData can be called for any unbound column, including those before the last
                //     bound column. Note that the columns must be called in order of ascending column number unless
                //     SQL_GD_ANY_ORDER is also returned.
                // SQL_GD_ANY_ORDER = SQLGetData can be called for unbound columns in any order. Note that SQLGetData
                //     can be called only for columns after the last bound column unless SQL_GD_ANY_COLUMN is also
                //     returned.
                // SQL_GD_BLOCK = SQLGetData can be called for an unbound column in any row in a block (where the rowset
                //     size is greater than 1) of data after positioning to that row with SQLSetPos.
                // SQL_GD_BOUND = SQLGetData can be called for bound columns in addition to unbound columns. A driver
                //     cannot return this value unless it also returns SQL_GD_ANY_COLUMN.
                // SQL_GD_OUTPUT_PARAMS = SQLGetData can be called to return output parameter values. For more
                //     information, see Retrieving Output.
                intParams[SQL_GETDATA_EXTENSIONS] = SQL_GD_ANY_COLUMN | SQL_GD_ANY_ORDER | SQL_GD_BOUND;
#endif // SQL_GETDATA_EXTENSIONS

#ifdef SQL_ODBC_INTERFACE_CONFORMANCE
                // Indicates the level of the ODBC 3.x interface that the driver
                // complies with.
                intParams[SQL_ODBC_INTERFACE_CONFORMANCE] = SQL_OIC_CORE;
#endif // SQL_ODBC_INTERFACE_CONFORMANCE

#ifdef SQL_SQL_CONFORMANCE
                // Indicates the level of SQL-92 supported by the driver.
                intParams[SQL_SQL_CONFORMANCE] = SQL_SC_SQL92_ENTRY;
#endif // SQL_SQL_CONFORMANCE

#ifdef SQL_CATALOG_USAGE
                // Bitmask enumerating the statements in which catalogs can be used.
                // The following bitmasks are used to determine where catalogs can be used:
                // SQL_CU_DML_STATEMENTS = Catalogs are supported in all Data Manipulation Language statements :
                //     SELECT, INSERT, UPDATE, DELETE, and if supported, SELECT FOR UPDATE and positioned update and
                //     delete statements.
                // SQL_CU_PROCEDURE_INVOCATION = Catalogs are supported in the ODBC procedure invocation statement.
                // SQL_CU_TABLE_DEFINITION = Catalogs are supported in all table definition statements : CREATE TABLE,
                //     CREATE VIEW, ALTER TABLE, DROP TABLE, and DROP VIEW.
                // SQL_CU_INDEX_DEFINITION = Catalogs are supported in all index definition statements : CREATE INDEX
                //     and DROP INDEX.
                // SQL_CU_PRIVILEGE_DEFINITION = Catalogs are supported in all privilege definition statements : GRANT
                //     and REVOKE.
                //
                // A value of 0 is returned if catalogs are not supported by the data source.To determine whether
                // catalogs are supported, an application calls SQLGetInfo with the SQL_CATALOG_NAME information type.
                // An SQL - 92 Full level-conformant driver will always return a bitmask with all of these bits set.
                // This InfoType has been renamed for ODBC 3.0 from the ODBC 2.0 InfoType SQL_QUALIFIER_USAGE.
                intParams[SQL_CATALOG_USAGE] = 0;
#endif // SQL_CATALOG_USAGE

#ifdef SQL_QUALIFIER_USAGE
                intParams[SQL_QUALIFIER_USAGE] = 0;
#endif // SQL_QUALIFIER_USAGE

#ifdef SQL_SCHEMA_USAGE
                // Bitmask enumerating the statements in which schemas can be used.
                intParams[SQL_SCHEMA_USAGE] = SQL_SU_DML_STATEMENTS | SQL_SU_TABLE_DEFINITION |
                    SQL_SU_PRIVILEGE_DEFINITION | SQL_SU_INDEX_DEFINITION;
#endif // SQL_SCHEMA_USAGE

#ifdef SQL_AGGREGATE_FUNCTIONS
                // Bitmask enumerating support for aggregation functions.
                intParams[SQL_AGGREGATE_FUNCTIONS] = SQL_AF_AVG | SQL_AF_COUNT | SQL_AF_MAX | SQL_AF_MIN | SQL_AF_SUM |
                    SQL_AF_DISTINCT;
#endif // SQL_AGGREGATE_FUNCTIONS

#ifdef SQL_NUMERIC_FUNCTIONS
                // Bitmask enumerating the scalar numeric functions supported by the driver and associated data source.
                intParams[SQL_NUMERIC_FUNCTIONS] = SQL_FN_NUM_ABS | SQL_FN_NUM_ACOS | SQL_FN_NUM_ASIN | SQL_FN_NUM_EXP |
                    SQL_FN_NUM_ATAN | SQL_FN_NUM_ATAN2 | SQL_FN_NUM_CEILING | SQL_FN_NUM_COS | SQL_FN_NUM_TRUNCATE |
                    SQL_FN_NUM_FLOOR | SQL_FN_NUM_DEGREES | SQL_FN_NUM_POWER | SQL_FN_NUM_RADIANS | SQL_FN_NUM_SIGN |
                    SQL_FN_NUM_SIN | SQL_FN_NUM_LOG | SQL_FN_NUM_TAN | SQL_FN_NUM_PI | SQL_FN_NUM_MOD | SQL_FN_NUM_COT |
                    SQL_FN_NUM_LOG10 | SQL_FN_NUM_ROUND | SQL_FN_NUM_SQRT | SQL_FN_NUM_RAND;
#endif // SQL_NUMERIC_FUNCTIONS

#ifdef SQL_STRING_FUNCTIONS
                // Bitmask enumerating the scalar string functions supported by the driver and associated data source.
                intParams[SQL_STRING_FUNCTIONS] = SQL_FN_STR_ASCII | SQL_FN_STR_BIT_LENGTH | SQL_FN_STR_CHAR_LENGTH |
                    SQL_FN_STR_CHAR | SQL_FN_STR_CONCAT | SQL_FN_STR_DIFFERENCE | SQL_FN_STR_INSERT | SQL_FN_STR_LEFT |
                    SQL_FN_STR_LENGTH | SQL_FN_STR_CHARACTER_LENGTH | SQL_FN_STR_LTRIM | SQL_FN_STR_OCTET_LENGTH |
                    SQL_FN_STR_POSITION | SQL_FN_STR_REPEAT | SQL_FN_STR_REPLACE | SQL_FN_STR_RIGHT | SQL_FN_STR_RTRIM |
                    SQL_FN_STR_SOUNDEX | SQL_FN_STR_SPACE | SQL_FN_STR_SUBSTRING | SQL_FN_STR_LCASE | SQL_FN_STR_UCASE |
                    SQL_FN_STR_LOCATE_2 | SQL_FN_STR_LOCATE;
#endif // SQL_STRING_FUNCTIONS

#ifdef SQL_TIMEDATE_FUNCTIONS
                // Bitmask enumerating the scalar date and time functions supported by the driver and associated data
                // source.
                intParams[SQL_TIMEDATE_FUNCTIONS] = SQL_FN_TD_CURRENT_DATE | SQL_FN_TD_CURRENT_TIME | SQL_FN_TD_WEEK |
                    SQL_FN_TD_QUARTER | SQL_FN_TD_SECOND | SQL_FN_TD_CURDATE | SQL_FN_TD_CURTIME | SQL_FN_TD_DAYNAME |
                    SQL_FN_TD_MINUTE | SQL_FN_TD_DAYOFWEEK | SQL_FN_TD_DAYOFYEAR | SQL_FN_TD_EXTRACT | SQL_FN_TD_HOUR |
                    SQL_FN_TD_DAYOFMONTH | SQL_FN_TD_MONTH | SQL_FN_TD_MONTHNAME | SQL_FN_TD_NOW | SQL_FN_TD_YEAR |
                    SQL_FN_TD_CURRENT_TIMESTAMP;
#endif // SQL_TIMEDATE_FUNCTIONS

#ifdef SQL_TIMEDATE_ADD_INTERVALS
                // Bitmask enumerating timestamp intervals supported by the driver and associated data source for the
                // TIMESTAMPADD scalar function.
                intParams[SQL_TIMEDATE_ADD_INTERVALS] = 0;
#endif // SQL_TIMEDATE_ADD_INTERVALS

#ifdef SQL_TIMEDATE_DIFF_INTERVALS
                // Bitmask enumerating timestamp intervals supported by the driver and associated data source for the
                // TIMESTAMPDIFF scalar function.
                intParams[SQL_TIMEDATE_DIFF_INTERVALS] = 0;
#endif // SQL_TIMEDATE_DIFF_INTERVALS

#ifdef SQL_DATETIME_LITERALS
                // Bitmask enumerating the SQL-92 datetime literals supported by the data source.
                intParams[SQL_DATETIME_LITERALS] =  SQL_DL_SQL92_DATE | SQL_DL_SQL92_TIME | SQL_DL_SQL92_TIMESTAMP;
#endif // SQL_DATETIME_LITERALS

#ifdef SQL_SYSTEM_FUNCTIONS
                // Bitmask enumerating the scalar system functions supported by the driver and associated data source.
                intParams[SQL_SYSTEM_FUNCTIONS] = SQL_FN_SYS_USERNAME | SQL_FN_SYS_DBNAME | SQL_FN_SYS_IFNULL;
#endif // SQL_SYSTEM_FUNCTIONS

#ifdef SQL_CONVERT_FUNCTIONS
                // Bitmask enumerating the scalar conversion functions supported by the driver and associated data
                // source.
                intParams[SQL_CONVERT_FUNCTIONS] = SQL_FN_CVT_CONVERT | SQL_FN_CVT_CAST;
#endif // SQL_CONVERT_FUNCTIONS

#ifdef SQL_OJ_CAPABILITIES
                // Bitmask enumerating the types of outer joins supported by the driver and data source.
                intParams[SQL_OJ_CAPABILITIES] = SQL_OJ_LEFT | SQL_OJ_NOT_ORDERED | SQL_OJ_ALL_COMPARISON_OPS;
#endif // SQL_OJ_CAPABILITIES

#ifdef SQL_POS_OPERATIONS
                // DEPRECATED. Included for backward-compatibility.
                // A bitmask enumerating the supported operations in SQLSetPos.
                //
                // SQL_POS_POSITION (ODBC 2.0)
                // SQL_POS_REFRESH (ODBC 2.0)
                // SQL_POS_UPDATE (ODBC 2.0)
                // SQL_POS_DELETE (ODBC 2.0)
                // SQL_POS_ADD (ODBC 2.0)
                intParams[SQL_POS_OPERATIONS] = 0;
#endif // SQL_POS_OPERATIONS

#ifdef SQL_SQL92_NUMERIC_VALUE_FUNCTIONS
                // Bitmask enumerating the numeric value scalar functions that are supported by the driver and the
                // associated data source, as defined in SQL-92.
                // The following bitmasks are used to determine which numeric functions are supported :
                // SQL_SNVF_BIT_LENGTH
                // SQL_SNVF_CHAR_LENGTH
                // SQL_SNVF_CHARACTER_LENGTH
                // SQL_SNVF_EXTRACT
                // SQL_SNVF_OCTET_LENGTH
                // SQL_SNVF_POSITION
                intParams[SQL_SQL92_NUMERIC_VALUE_FUNCTIONS] = SQL_SNVF_BIT_LENGTH | SQL_SNVF_CHARACTER_LENGTH |
                    SQL_SNVF_EXTRACT | SQL_SNVF_OCTET_LENGTH | SQL_SNVF_POSITION;
#endif // SQL_SQL92_NUMERIC_VALUE_FUNCTIONS

#ifdef SQL_SQL92_STRING_FUNCTIONS
                // Bitmask enumerating the string scalar functions.
                intParams[SQL_SQL92_STRING_FUNCTIONS] = SQL_SSF_LOWER | SQL_SSF_UPPER | SQL_SSF_TRIM_TRAILING |
                    SQL_SSF_SUBSTRING | SQL_SSF_TRIM_BOTH | SQL_SSF_TRIM_LEADING;
#endif // SQL_SQL92_STRING_FUNCTIONS

#ifdef SQL_SQL92_DATETIME_FUNCTIONS
                // Bitmask enumerating the datetime scalar functions.
                intParams[SQL_SQL92_DATETIME_FUNCTIONS] = SQL_SDF_CURRENT_DATE |
                    SQL_SDF_CURRENT_TIMESTAMP;
#endif // SQL_SQL92_DATETIME_FUNCTIONS

#ifdef SQL_SQL92_VALUE_EXPRESSIONS
                // Bitmask enumerating the value expressions supported, as defined in SQL-92.
                intParams[SQL_SQL92_VALUE_EXPRESSIONS] = SQL_SVE_CASE |
                    SQL_SVE_CAST | SQL_SVE_COALESCE | SQL_SVE_NULLIF;
#endif // SQL_SQL92_VALUE_EXPRESSIONS

#ifdef SQL_SQL92_PREDICATES
                // Bitmask enumerating the datetime scalar functions.
                intParams[SQL_SQL92_PREDICATES] = SQL_SP_BETWEEN | SQL_SP_COMPARISON | SQL_SP_EXISTS | SQL_SP_IN |
                    SQL_SP_ISNOTNULL | SQL_SP_ISNULL | SQL_SP_LIKE | SQL_SP_MATCH_FULL | SQL_SP_MATCH_PARTIAL |
                    SQL_SP_MATCH_UNIQUE_FULL | SQL_SP_MATCH_UNIQUE_PARTIAL | SQL_SP_OVERLAPS | SQL_SP_UNIQUE |
                    SQL_SP_QUANTIFIED_COMPARISON;
#endif // SQL_SQL92_PREDICATES

#ifdef SQL_SQL92_RELATIONAL_JOIN_OPERATORS
                // Bitmask enumerating the relational join operators supported in a SELECT statement, as defined
                // in SQL-92.
                intParams[SQL_SQL92_RELATIONAL_JOIN_OPERATORS] = SQL_SRJO_CORRESPONDING_CLAUSE | SQL_SRJO_CROSS_JOIN |
                    SQL_SRJO_EXCEPT_JOIN | SQL_SRJO_INNER_JOIN | SQL_SRJO_LEFT_OUTER_JOIN| SQL_SRJO_RIGHT_OUTER_JOIN |
                    SQL_SRJO_NATURAL_JOIN | SQL_SRJO_INTERSECT_JOIN | SQL_SRJO_UNION_JOIN;
#endif // SQL_SQL92_RELATIONAL_JOIN_OPERATORS

#ifdef SQL_STATIC_CURSOR_ATTRIBUTES1
                // Bitmask that describes the attributes of a static cursor that are supported by the driver. This
                // bitmask contains the first subset of attributes; for the second subset, see
                // SQL_STATIC_CURSOR_ATTRIBUTES2.
                intParams[SQL_STATIC_CURSOR_ATTRIBUTES1] = SQL_CA1_NEXT | SQL_CA1_ABSOLUTE;
#endif // SQL_STATIC_CURSOR_ATTRIBUTES1

#ifdef SQL_STATIC_CURSOR_ATTRIBUTES2
                // Bitmask that describes the attributes of a static cursor that are supported by the driver. This
                // bitmask contains the second subset of attributes; for the first subset, see
                // SQL_STATIC_CURSOR_ATTRIBUTES1.
                intParams[SQL_STATIC_CURSOR_ATTRIBUTES2] = 0;
#endif // SQL_STATIC_CURSOR_ATTRIBUTES2

#ifdef SQL_CONVERT_BIGINT
                // Bitmask indicates the conversions supported by the CONVERT scalar function for target type BIGINT
                intParams[SQL_CONVERT_BIGINT] = SQL_CVT_CHAR | SQL_CVT_VARCHAR | SQL_CVT_LONGVARCHAR | SQL_CVT_WCHAR |
                    SQL_CVT_WLONGVARCHAR | SQL_CVT_WVARCHAR | SQL_CVT_NUMERIC | SQL_CVT_TIMESTAMP | SQL_CVT_TINYINT |
                    SQL_CVT_SMALLINT | SQL_CVT_INTEGER | SQL_CVT_BIGINT | SQL_CVT_BIT;
#endif // SQL_CONVERT_BIGINT

#ifdef SQL_CONVERT_BINARY
                // Bitmask indicates the conversions supported by the CONVERT scalar function for target type BINARY
                intParams[SQL_CONVERT_BINARY] = SQL_CVT_CHAR | SQL_CVT_VARCHAR | SQL_CVT_LONGVARCHAR | SQL_CVT_BIT |
                    SQL_CVT_WCHAR | SQL_CVT_WLONGVARCHAR | SQL_CVT_WVARCHAR | SQL_CVT_NUMERIC | SQL_CVT_LONGVARBINARY |
                    SQL_CVT_FLOAT | SQL_CVT_SMALLINT | SQL_CVT_INTEGER | SQL_CVT_BIGINT | SQL_CVT_REAL | SQL_CVT_DATE |
                    SQL_CVT_TINYINT | SQL_CVT_DOUBLE | SQL_CVT_BINARY | SQL_CVT_DECIMAL | SQL_CVT_TIME | SQL_CVT_GUID |
                    SQL_CVT_TIMESTAMP | SQL_CVT_VARBINARY;
#endif // SQL_CONVERT_BINARY

#ifdef SQL_CONVERT_BIT
                // Bitmask indicates the conversions supported by the CONVERT scalar function for target type BIT
                intParams[SQL_CONVERT_BIT] = SQL_CVT_BIGINT | SQL_CVT_VARCHAR | SQL_CVT_LONGVARCHAR | SQL_CVT_WVARCHAR |
                    SQL_CVT_NUMERIC | SQL_CVT_BIT | SQL_CVT_CHAR | SQL_CVT_SMALLINT | SQL_CVT_INTEGER | SQL_CVT_TINYINT;
#endif // SQL_CONVERT_BIT

#ifdef SQL_CONVERT_CHAR
                // Bitmask indicates the conversions supported by the CONVERT scalar function for target type CHAR
                intParams[SQL_CONVERT_CHAR] = SQL_CVT_CHAR | SQL_CVT_VARCHAR | SQL_CVT_LONGVARCHAR | SQL_CVT_VARBINARY |
                    SQL_CVT_WCHAR | SQL_CVT_WLONGVARCHAR | SQL_CVT_WVARCHAR | SQL_CVT_NUMERIC | SQL_CVT_DECIMAL |
                    SQL_CVT_BIT | SQL_CVT_TINYINT | SQL_CVT_SMALLINT | SQL_CVT_INTEGER | SQL_CVT_BIGINT | SQL_CVT_REAL |
                    SQL_CVT_FLOAT | SQL_CVT_DOUBLE | SQL_CVT_BINARY | SQL_CVT_TIMESTAMP | SQL_CVT_DATE | SQL_CVT_TIME |
                    SQL_CVT_LONGVARBINARY | SQL_CVT_GUID;
#endif // SQL_CONVERT_CHAR

#ifdef SQL_CONVERT_VARCHAR
                // Bitmask indicates the conversions supported by the CONVERT scalar function for target type VARCHAR
                intParams[SQL_CONVERT_VARCHAR] = SQL_CVT_CHAR | SQL_CVT_VARCHAR | SQL_CVT_LONGVARCHAR | SQL_CVT_WCHAR |
                    SQL_CVT_WLONGVARCHAR | SQL_CVT_WVARCHAR | SQL_CVT_NUMERIC | SQL_CVT_DECIMAL | SQL_CVT_TINYINT |
                    SQL_CVT_SMALLINT | SQL_CVT_INTEGER | SQL_CVT_BIGINT | SQL_CVT_REAL | SQL_CVT_FLOAT | SQL_CVT_BIT |
                    SQL_CVT_DOUBLE | SQL_CVT_BINARY | SQL_CVT_VARBINARY | SQL_CVT_GUID | SQL_CVT_DATE | SQL_CVT_TIME |
                    SQL_CVT_LONGVARBINARY | SQL_CVT_TIMESTAMP;
#endif // SQL_CONVERT_VARCHAR

#ifdef SQL_CONVERT_LONGVARCHAR
                // Bitmask indicates the conversions supported by the CONVERT scalar function for target type
                // LONGVARCHAR
                intParams[SQL_CONVERT_LONGVARCHAR] = SQL_CVT_CHAR | SQL_CVT_VARCHAR | SQL_CVT_DATE | SQL_CVT_TIME |
                    SQL_CVT_LONGVARCHAR | SQL_CVT_WCHAR | SQL_CVT_WLONGVARCHAR | SQL_CVT_WVARCHAR | SQL_CVT_NUMERIC |
                    SQL_CVT_DECIMAL | SQL_CVT_BIT | SQL_CVT_REAL | SQL_CVT_SMALLINT | SQL_CVT_INTEGER | SQL_CVT_GUID |
                    SQL_CVT_BIGINT | SQL_CVT_LONGVARBINARY | SQL_CVT_DOUBLE | SQL_CVT_BINARY | SQL_CVT_VARBINARY |
                    SQL_CVT_TINYINT | SQL_CVT_FLOAT | SQL_CVT_TIMESTAMP;
#endif // SQL_CONVERT_LONGVARCHAR

#ifdef SQL_CONVERT_WCHAR
                // Bitmask indicates the conversions supported by the CONVERT scalar function for target type WCHAR
                intParams[SQL_CONVERT_WCHAR] = SQL_CVT_CHAR | SQL_CVT_VARCHAR | SQL_CVT_LONGVARCHAR | SQL_CVT_WCHAR |
                    SQL_CVT_WLONGVARCHAR | SQL_CVT_WVARCHAR | SQL_CVT_NUMERIC | SQL_CVT_DECIMAL | SQL_CVT_VARBINARY |
                    SQL_CVT_BIT | SQL_CVT_TINYINT | SQL_CVT_SMALLINT | SQL_CVT_INTEGER | SQL_CVT_BIGINT | SQL_CVT_REAL |
                    SQL_CVT_FLOAT | SQL_CVT_DOUBLE | SQL_CVT_BINARY | SQL_CVT_LONGVARBINARY | SQL_CVT_TIMESTAMP |
                    SQL_CVT_DATE | SQL_CVT_TIME | SQL_CVT_GUID;
#endif // SQL_CONVERT_WCHAR

#ifdef SQL_CONVERT_WVARCHAR
                // Bitmask indicates the conversions supported by the CONVERT scalar function for target type WVARCHAR
                intParams[SQL_CONVERT_WVARCHAR] = SQL_CVT_CHAR | SQL_CVT_VARCHAR | SQL_CVT_LONGVARCHAR | SQL_CVT_REAL |
                    SQL_CVT_WCHAR | SQL_CVT_WLONGVARCHAR | SQL_CVT_WVARCHAR | SQL_CVT_NUMERIC | SQL_CVT_LONGVARBINARY |
                    SQL_CVT_DECIMAL | SQL_CVT_BIT | SQL_CVT_TINYINT | SQL_CVT_SMALLINT | SQL_CVT_DATE | SQL_CVT_TIME |
                    SQL_CVT_INTEGER | SQL_CVT_BIGINT | SQL_CVT_FLOAT | SQL_CVT_DOUBLE | SQL_CVT_BINARY | SQL_CVT_GUID |
                    SQL_CVT_VARBINARY | SQL_CVT_TIMESTAMP;
#endif // SQL_CONVERT_WVARCHAR

#ifdef SQL_CONVERT_WLONGVARCHAR
                // Bitmask indicates the conversions supported by the CONVERT scalar function for target type
                // WLONGVARCHAR
                intParams[SQL_CONVERT_WLONGVARCHAR] = SQL_CVT_CHAR | SQL_CVT_VARCHAR | SQL_CVT_BIGINT | SQL_CVT_REAL |
                    SQL_CVT_LONGVARCHAR | SQL_CVT_WCHAR | SQL_CVT_WLONGVARCHAR | SQL_CVT_WVARCHAR | SQL_CVT_VARBINARY |
                    SQL_CVT_NUMERIC | SQL_CVT_DECIMAL | SQL_CVT_BIT | SQL_CVT_TINYINT | SQL_CVT_DATE | SQL_CVT_FLOAT |
                    SQL_CVT_INTEGER | SQL_CVT_SMALLINT | SQL_CVT_DOUBLE | SQL_CVT_BINARY | SQL_CVT_LONGVARBINARY |
                    SQL_CVT_TIME | SQL_CVT_TIMESTAMP | SQL_CVT_GUID;
#endif // SQL_CONVERT_WLONGVARCHAR

#ifdef SQL_CONVERT_GUID
                // Bitmask indicates the conversions supported by the CONVERT scalar function for target type GUID
                intParams[SQL_CONVERT_GUID] = SQL_CVT_CHAR | SQL_CVT_VARCHAR | SQL_CVT_LONGVARCHAR | SQL_CVT_WVARCHAR |
                    SQL_CVT_WCHAR | SQL_CVT_WLONGVARCHAR | SQL_CVT_BINARY | SQL_CVT_VARBINARY | SQL_CVT_LONGVARBINARY |
                    SQL_CVT_GUID;
#endif // SQL_CONVERT_GUID

#ifdef SQL_CONVERT_DATE
                // Bitmask indicates the conversions supported by the CONVERT scalar function for target type DATE
                intParams[SQL_CONVERT_DATE] = SQL_CVT_CHAR | SQL_CVT_VARCHAR | SQL_CVT_LONGVARCHAR | SQL_CVT_WCHAR |
                    SQL_CVT_WLONGVARCHAR | SQL_CVT_WVARCHAR | SQL_CVT_NUMERIC | SQL_CVT_INTEGER | SQL_CVT_BIGINT |
                    SQL_CVT_BINARY | SQL_CVT_VARBINARY | SQL_CVT_LONGVARBINARY | SQL_CVT_DATE | SQL_CVT_TIMESTAMP;
#endif // SQL_CONVERT_DATE

#ifdef SQL_CONVERT_DECIMAL
                // Bitmask indicates the conversions supported by the CONVERT scalar function for target type DECIMAL
                intParams[SQL_CONVERT_DECIMAL] = SQL_CVT_CHAR | SQL_CVT_VARCHAR | SQL_CVT_LONGVARCHAR | SQL_CVT_WCHAR |
                    SQL_CVT_WLONGVARCHAR | SQL_CVT_WVARCHAR | SQL_CVT_NUMERIC | SQL_CVT_DECIMAL | SQL_CVT_TIMESTAMP |
                    SQL_CVT_BIT | SQL_CVT_TINYINT | SQL_CVT_SMALLINT | SQL_CVT_INTEGER | SQL_CVT_BIGINT | SQL_CVT_REAL |
                    SQL_CVT_FLOAT | SQL_CVT_DOUBLE | SQL_CVT_BINARY | SQL_CVT_VARBINARY | SQL_CVT_LONGVARBINARY;
#endif // SQL_CONVERT_DECIMAL

#ifdef SQL_CONVERT_DOUBLE
                // Bitmask indicates the conversions supported by the CONVERT scalar function for target type DOUBLE
                intParams[SQL_CONVERT_DOUBLE] = SQL_CVT_CHAR | SQL_CVT_VARCHAR | SQL_CVT_LONGVARCHAR | SQL_CVT_WCHAR |
                    SQL_CVT_WLONGVARCHAR | SQL_CVT_WVARCHAR | SQL_CVT_NUMERIC | SQL_CVT_DECIMAL | SQL_CVT_TIMESTAMP |
                    SQL_CVT_TINYINT | SQL_CVT_BIGINT | SQL_CVT_INTEGER | SQL_CVT_FLOAT | SQL_CVT_REAL | SQL_CVT_DOUBLE |
                    SQL_CVT_BINARY | SQL_CVT_VARBINARY | SQL_CVT_SMALLINT | SQL_CVT_LONGVARBINARY;
#endif // SQL_CONVERT_DOUBLE

#ifdef SQL_CONVERT_FLOAT
                // Bitmask indicates the conversions supported by the CONVERT scalar function for target type FLOAT
                intParams[SQL_CONVERT_FLOAT] = SQL_CVT_CHAR | SQL_CVT_VARCHAR | SQL_CVT_LONGVARCHAR | SQL_CVT_WVARCHAR |
                    SQL_CVT_WLONGVARCHAR | SQL_CVT_NUMERIC | SQL_CVT_DECIMAL | SQL_CVT_TINYINT | SQL_CVT_SMALLINT |
                    SQL_CVT_INTEGER | SQL_CVT_BIGINT | SQL_CVT_REAL | SQL_CVT_FLOAT | SQL_CVT_DOUBLE | SQL_CVT_BINARY |
                    SQL_CVT_VARBINARY | SQL_CVT_WCHAR | SQL_CVT_LONGVARBINARY | SQL_CVT_TIMESTAMP | SQL_CVT_BIT;
#endif // SQL_CONVERT_FLOAT

#ifdef SQL_CONVERT_REAL
                // Bitmask indicates the conversions supported by the CONVERT scalar function for target type REAL
                intParams[SQL_CONVERT_REAL] = SQL_CVT_CHAR | SQL_CVT_VARCHAR | SQL_CVT_LONGVARCHAR | SQL_CVT_WVARCHAR |
                    SQL_CVT_NUMERIC | SQL_CVT_DECIMAL | SQL_CVT_BIT | SQL_CVT_FLOAT | SQL_CVT_SMALLINT | SQL_CVT_REAL |
                    SQL_CVT_INTEGER | SQL_CVT_BIGINT | SQL_CVT_DOUBLE | SQL_CVT_TINYINT | SQL_CVT_WLONGVARCHAR |
                    SQL_CVT_BINARY | SQL_CVT_VARBINARY | SQL_CVT_LONGVARBINARY | SQL_CVT_TIMESTAMP | SQL_CVT_WCHAR;
#endif // SQL_CONVERT_REAL

#ifdef SQL_CONVERT_INTEGER
                // Bitmask indicates the conversions supported by the CONVERT scalar function for target type INTEGER
                intParams[SQL_CONVERT_INTEGER] = SQL_CVT_CHAR | SQL_CVT_VARCHAR | SQL_CVT_LONGVARCHAR | SQL_CVT_WCHAR |
                    SQL_CVT_WLONGVARCHAR | SQL_CVT_WVARCHAR | SQL_CVT_NUMERIC | SQL_CVT_DECIMAL | SQL_CVT_TINYINT |
                    SQL_CVT_SMALLINT | SQL_CVT_BIT | SQL_CVT_INTEGER | SQL_CVT_BIGINT | SQL_CVT_REAL | SQL_CVT_FLOAT |
                    SQL_CVT_DOUBLE | SQL_CVT_BINARY | SQL_CVT_VARBINARY | SQL_CVT_LONGVARBINARY | SQL_CVT_TIMESTAMP;
#endif // SQL_CONVERT_INTEGER

#ifdef SQL_CONVERT_NUMERIC
                // Bitmask indicates the conversions supported by the CONVERT scalar function for target type NUMERIC
                intParams[SQL_CONVERT_NUMERIC] = SQL_CVT_CHAR | SQL_CVT_VARCHAR | SQL_CVT_LONGVARCHAR | SQL_CVT_WCHAR |
                    SQL_CVT_WLONGVARCHAR | SQL_CVT_WVARCHAR | SQL_CVT_NUMERIC | SQL_CVT_DECIMAL | SQL_CVT_SMALLINT |
                    SQL_CVT_INTEGER | SQL_CVT_BIGINT | SQL_CVT_REAL | SQL_CVT_BIT | SQL_CVT_TINYINT | SQL_CVT_FLOAT |
                    SQL_CVT_DOUBLE | SQL_CVT_BINARY | SQL_CVT_VARBINARY | SQL_CVT_LONGVARBINARY | SQL_CVT_TIMESTAMP;
#endif // SQL_CONVERT_NUMERIC

#ifdef SQL_CONVERT_SMALLINT
                // Bitmask indicates the conversions supported by the CONVERT scalar function for target type SMALLINT
                intParams[SQL_CONVERT_SMALLINT] = SQL_CVT_CHAR | SQL_CVT_VARCHAR | SQL_CVT_LONGVARCHAR | SQL_CVT_WCHAR |
                    SQL_CVT_WLONGVARCHAR | SQL_CVT_WVARCHAR | SQL_CVT_NUMERIC | SQL_CVT_DECIMAL | SQL_CVT_VARBINARY |
                    SQL_CVT_BIT | SQL_CVT_TINYINT | SQL_CVT_SMALLINT | SQL_CVT_INTEGER | SQL_CVT_BIGINT | SQL_CVT_REAL |
                    SQL_CVT_FLOAT | SQL_CVT_DOUBLE | SQL_CVT_BINARY | SQL_CVT_LONGVARBINARY | SQL_CVT_TIMESTAMP;
#endif // SQL_CONVERT_SMALLINT

#ifdef SQL_CONVERT_TINYINT
                // Bitmask indicates the conversions supported by the CONVERT scalar function for target type TINYINT
                intParams[SQL_CONVERT_TINYINT] = SQL_CVT_CHAR | SQL_CVT_VARCHAR | SQL_CVT_LONGVARCHAR | SQL_CVT_WCHAR |
                    SQL_CVT_WLONGVARCHAR | SQL_CVT_WVARCHAR | SQL_CVT_NUMERIC | SQL_CVT_DECIMAL | SQL_CVT_TINYINT |
                    SQL_CVT_SMALLINT | SQL_CVT_BIT | SQL_CVT_INTEGER | SQL_CVT_BIGINT | SQL_CVT_REAL | SQL_CVT_FLOAT |
                    SQL_CVT_DOUBLE | SQL_CVT_BINARY | SQL_CVT_VARBINARY | SQL_CVT_LONGVARBINARY | SQL_CVT_TIMESTAMP;
#endif // SQL_CONVERT_TINYINT

#ifdef SQL_CONVERT_TIME
                // Bitmask indicates the conversions supported by the CONVERT scalar function for target type TIME
                intParams[SQL_CONVERT_TIME] = SQL_CVT_CHAR | SQL_CVT_VARCHAR | SQL_CVT_LONGVARCHAR | SQL_CVT_WVARCHAR |
                    SQL_CVT_WCHAR | SQL_CVT_WLONGVARCHAR | SQL_CVT_BINARY | SQL_CVT_VARBINARY | SQL_CVT_LONGVARBINARY |
                    SQL_CVT_TIME | SQL_CVT_TIMESTAMP;
#endif // SQL_CONVERT_TIME

#ifdef SQL_CONVERT_TIMESTAMP
                // Bitmask indicates the conversions supported by the CONVERT scalar function for target type TIMESTAMP
                intParams[SQL_CONVERT_TIMESTAMP] = SQL_CVT_CHAR | SQL_CVT_VARCHAR | SQL_CVT_LONGVARCHAR | SQL_CVT_DATE |
                    SQL_CVT_WCHAR | SQL_CVT_WLONGVARCHAR | SQL_CVT_WVARCHAR | SQL_CVT_NUMERIC | SQL_CVT_LONGVARBINARY |
                    SQL_CVT_DECIMAL | SQL_CVT_INTEGER | SQL_CVT_BINARY | SQL_CVT_VARBINARY | SQL_CVT_TIMESTAMP |
                    SQL_CVT_BIGINT | SQL_CVT_TIME;
#endif // SQL_CONVERT_TIMESTAMP

#ifdef SQL_CONVERT_VARBINARY
                // Bitmask indicates the conversions supported by the CONVERT scalar function for target type VARBINARY
                intParams[SQL_CONVERT_VARBINARY] = SQL_CVT_CHAR | SQL_CVT_VARCHAR | SQL_CVT_LONGVARCHAR | SQL_CVT_BIT |
                    SQL_CVT_WCHAR | SQL_CVT_WLONGVARCHAR | SQL_CVT_WVARCHAR | SQL_CVT_NUMERIC | SQL_CVT_DECIMAL |
                    SQL_CVT_TINYINT | SQL_CVT_SMALLINT | SQL_CVT_DATE | SQL_CVT_BIGINT | SQL_CVT_REAL | SQL_CVT_FLOAT |
                    SQL_CVT_DOUBLE | SQL_CVT_INTEGER | SQL_CVT_BINARY | SQL_CVT_VARBINARY | SQL_CVT_LONGVARBINARY |
                    SQL_CVT_TIME | SQL_CVT_TIMESTAMP | SQL_CVT_GUID;
#endif // SQL_CONVERT_VARBINARY

#ifdef SQL_CONVERT_LONGVARBINARY
                // Bitmask indicates the conversions supported by the CONVERT scalar function for target type
                // LONGVARBINARY
                intParams[SQL_CONVERT_LONGVARBINARY] = SQL_CVT_CHAR | SQL_CVT_VARCHAR | SQL_CVT_BIT | SQL_CVT_TINYINT |
                    SQL_CVT_WCHAR | SQL_CVT_WLONGVARCHAR | SQL_CVT_WVARCHAR | SQL_CVT_LONGVARCHAR | SQL_CVT_NUMERIC |
                    SQL_CVT_DECIMAL | SQL_CVT_FLOAT | SQL_CVT_INTEGER | SQL_CVT_BIGINT | SQL_CVT_REAL | SQL_CVT_DATE |
                    SQL_CVT_DOUBLE | SQL_CVT_BINARY | SQL_CVT_VARBINARY | SQL_CVT_LONGVARBINARY | SQL_CVT_TIMESTAMP |
                    SQL_CVT_SMALLINT | SQL_CVT_TIME | SQL_CVT_GUID;
#endif // SQL_CONVERT_LONGVARBINARY

#ifdef SQL_PARAM_ARRAY_ROW_COUNTS
                // Enumerating the driver's properties regarding the availability of row counts in a parameterized
                // execution. Has the following values:
                //
                // SQL_PARC_BATCH = Individual row counts are available for each set of parameters. This is conceptually
                //     equivalent to the driver generating a batch of SQL statements, one for each parameter set in the
                //     array. Extended error information can be retrieved by using the SQL_PARAM_STATUS_PTR descriptor
                //     field.
                //
                // SQL_PARC_NO_BATCH = There is only one row count available, which is the cumulative row count
                //     resulting from the execution of the statement for the entire array of parameters. This is
                //     conceptually equivalent to treating the statement together with the complete parameter array as
                //     one atomic unit. Errors are handled the same as if one statement were executed.
                intParams[SQL_PARAM_ARRAY_ROW_COUNTS] = SQL_PARC_BATCH;
#endif // SQL_PARAM_ARRAY_ROW_COUNTS

#ifdef SQL_PARAM_ARRAY_SELECTS
                // Enumerating the driver's properties regarding the availability of result sets in a parameterized
                // execution. Has the following values:
                //
                // SQL_PAS_BATCH = There is one result set available per set of parameters. This is conceptually
                //     equivalent to the driver generating a batch of SQL statements, one for each parameter set in
                //     the array.
                //
                // SQL_PAS_NO_BATCH = There is only one result set available, which represents the cumulative result set
                //     resulting from the execution of the statement for the complete array of parameters. This is
                //     conceptually equivalent to treating the statement together with the complete parameter array as
                //     one atomic unit.
                //
                // SQL_PAS_NO_SELECT = A driver does not allow a result - set generating statement to be executed with
                //     an array of parameters.
                intParams[SQL_PARAM_ARRAY_SELECTS] = SQL_PAS_NO_SELECT;
#endif // SQL_PARAM_ARRAY_SELECTS

#ifdef SQL_SCROLL_OPTIONS
                // Bitmask enumerating the scroll options supported for scrollable cursors
                // SQL_SO_FORWARD_ONLY = The cursor only scrolls forward. (ODBC 1.0)
                // SQL_SO_STATIC = The data in the result set is static. (ODBC 2.0)
                // SQL_SO_KEYSET_DRIVEN = The driver saves and uses the keys for every row in the result set. (ODBC 1.0)
                // SQL_SO_DYNAMIC = The driver keeps the keys for every row in the rowset(the keyset size is the same
                //     as the rowset size). (ODBC 1.0)
                // SQL_SO_MIXED = The driver keeps the keys for every row in the keyset, and the keyset size is greater
                //     than the rowset size.The cursor is keyset - driven inside the keyset and dynamic outside the
                //     keyset. (ODBC 1.0)
                intParams[SQL_SCROLL_OPTIONS] = SQL_SO_FORWARD_ONLY | SQL_SO_STATIC;
#endif // SQL_SCROLL_OPTIONS

#ifdef SQL_ALTER_DOMAIN
                // Bitmask enumerating the clauses in the ALTER DOMAIN statement, as defined in SQL-92, supported by the
                // data source. An SQL-92 Full level-compliant driver will always return all the bitmasks. A return
                // value of "0" means that the ALTER DOMAIN statement is not supported.
                //
                // The SQL - 92 or FIPS conformance level at which this feature must be supported is shown in
                // parentheses next to each bitmask.
                //
                // The following bitmasks are used to determine which clauses are supported :
                // SQL_AD_ADD_DOMAIN_CONSTRAINT = Adding a domain constraint is supported (Full level).
                // SQL_AD_ADD_DOMAIN_DEFAULT = <alter domain> <set domain default clause> is supported (Full level).
                // SQL_AD_CONSTRAINT_NAME_DEFINITION = <constraint name definition clause> is supported for naming
                //     domain constraint (Intermediate level).
                // SQL_AD_DROP_DOMAIN_CONSTRAINT = <drop domain constraint clause> is supported (Full level).
                // SQL_AD_DROP_DOMAIN_DEFAULT = <alter domain> <drop domain default clause> is supported (Full level).
                //
                // The following bits specify the supported <constraint attributes> if <add domain constraint> is
                // supported (the SQL_AD_ADD_DOMAIN_CONSTRAINT bit is set) :
                // SQL_AD_ADD_CONSTRAINT_DEFERRABLE (Full level)
                // SQL_AD_ADD_CONSTRAINT_NON_DEFERRABLE (Full level)
                // SQL_AD_ADD_CONSTRAINT_INITIALLY_DEFERRED (Full level)
                // SQL_AD_ADD_CONSTRAINT_INITIALLY_IMMEDIATE (Full level)
                intParams[SQL_ALTER_DOMAIN] = 0;
#endif // SQL_ALTER_DOMAIN

#ifdef SQL_ALTER_TABLE
                // Bitmask enumerating the clauses in the ALTER TABLE statement supported by the data source.
                //
                // The SQL - 92 or FIPS conformance level at which this feature must be supported is shown in
                // parentheses next to each bitmask. The following bitmasks are used to determine which clauses are
                // supported :
                // SQL_AT_ADD_COLUMN_COLLATION = <add column> clause is supported, with facility to specify column
                //     collation (Full level) (ODBC 3.0)
                // SQL_AT_ADD_COLUMN_DEFAULT = <add column> clause is supported, with facility to specify column
                //     defaults (FIPS Transitional level) (ODBC 3.0)
                // SQL_AT_ADD_COLUMN_SINGLE = <add column> is supported (FIPS Transitional level) (ODBC 3.0)
                // SQL_AT_ADD_CONSTRAINT = <add column> clause is supported, with facility to specify column
                //     constraints (FIPS Transitional level) (ODBC 3.0)
                // SQL_AT_ADD_TABLE_CONSTRAINT = <add table constraint> clause is supported(FIPS Transitional level)
                //     (ODBC 3.0)
                // SQL_AT_CONSTRAINT_NAME_DEFINITION = <constraint name definition> is supported for naming column and
                //     table constraints(Intermediate level) (ODBC 3.0)
                // SQL_AT_DROP_COLUMN_CASCADE = <drop column> CASCADE is supported (FIPS Transitional level) (ODBC 3.0)
                // SQL_AT_DROP_COLUMN_DEFAULT = <alter column> <drop column default clause> is supported (Intermediate
                //     level) (ODBC 3.0)
                // SQL_AT_DROP_COLUMN_RESTRICT = <drop column> RESTRICT is supported (FIPS Transitional level)
                //     (ODBC 3.0)
                // SQL_AT_DROP_TABLE_CONSTRAINT_CASCADE (ODBC 3.0)
                // SQL_AT_DROP_TABLE_CONSTRAINT_RESTRICT = <drop column> RESTRICT is supported(FIPS Transitional level)
                //     (ODBC 3.0)
                // SQL_AT_SET_COLUMN_DEFAULT = <alter column> <set column default clause> is supported (Intermediate
                //     level) (ODBC 3.0)
                //
                // The following bits specify the support <constraint attributes> if specifying column or table
                // constraints is supported (the SQL_AT_ADD_CONSTRAINT bit is set) :
                // SQL_AT_CONSTRAINT_INITIALLY_DEFERRED (Full level) (ODBC 3.0)
                // SQL_AT_CONSTRAINT_INITIALLY_IMMEDIATE (Full level) (ODBC 3.0)
                // SQL_AT_CONSTRAINT_DEFERRABLE (Full level) (ODBC 3.0)
                // SQL_AT_CONSTRAINT_NON_DEFERRABLE (Full level) (ODBC 3.0)
                intParams[SQL_ALTER_TABLE] = SQL_AT_ADD_COLUMN_SINGLE;
#endif // SQL_ALTER_TABLE

#ifdef SQL_CREATE_ASSERTION
                // Bitmask enumerating the clauses in the CREATE ASSERTION statement, as defined in SQL-92, supported by
                // the data source.
                //
                // The following bitmasks are used to determine which clauses are supported :
                // SQL_CA_CREATE_ASSERTION
                //
                // The following bits specify the supported constraint attribute if the ability to specify constraint
                // attributes explicitly is supported(see the SQL_ALTER_TABLE and SQL_CREATE_TABLE information types) :
                // SQL_CA_CONSTRAINT_INITIALLY_DEFERRED
                // SQL_CA_CONSTRAINT_INITIALLY_IMMEDIATE
                // SQL_CA_CONSTRAINT_DEFERRABLE
                // SQL_CA_CONSTRAINT_NON_DEFERRABLE
                //
                // An SQL - 92 Full level-conformant driver will always return all of these options as supported.
                // A return value of "0" means that the CREATE ASSERTION statement is not supported.
                intParams[SQL_CREATE_ASSERTION] = 0;
#endif // SQL_CREATE_ASSERTION

#ifdef SQL_CREATE_CHARACTER_SET
                // Bitmask enumerating the clauses in the CREATE CHARACTER SET statement, as defined in SQL-92,
                // supported by the data source.
                // The following bitmasks are used to determine which clauses are supported :
                // SQL_CCS_CREATE_CHARACTER_SET
                // SQL_CCS_COLLATE_CLAUSE
                // SQL_CCS_LIMITED_COLLATION
                //
                // An SQL - 92 Full level-conformant driver will always return all of these options as supported.
                // A return value of "0" means that the CREATE CHARACTER SET statement is not supported.
                intParams[SQL_CREATE_CHARACTER_SET] = 0;
#endif // SQL_CREATE_CHARACTER_SET

#ifdef SQL_CREATE_COLLATION
                // Bitmask enumerating the clauses in the CREATE COLLATION statement, as defined in SQL-92, supported by
                // the data source.
                // The following bitmask is used to determine which clauses are supported :
                // SQL_CCOL_CREATE_COLLATION
                // An SQL - 92 Full level-conformant driver will always return this option as supported.A return value
                // of "0" means that the CREATE COLLATION statement is not supported.
                intParams[SQL_CREATE_COLLATION] = 0;
#endif // SQL_CREATE_COLLATION

#ifdef SQL_CREATE_DOMAIN
                // Bitmask enumerating the clauses in the CREATE DOMAIN statement, as defined in SQL-92, supported by
                // the data source.
                // The following bitmasks are used to determine which clauses are supported :
                // SQL_CDO_CREATE_DOMAIN = The CREATE DOMAIN statement is supported(Intermediate level).
                // SQL_CDO_CONSTRAINT_NAME_DEFINITION = <constraint name definition> is supported for naming domain
                //     constraints(Intermediate level).
                //
                // The following bits specify the ability to create column constraints :
                // SQL_CDO_DEFAULT = Specifying domain constraints is supported (Intermediate level)
                // SQL_CDO_CONSTRAINT = Specifying domain defaults is supported (Intermediate level)
                // SQL_CDO_COLLATION = Specifying domain collation is supported (Full level)
                //
                // The following bits specify the supported constraint attributes if specifying domain constraints is
                // supported (SQL_CDO_DEFAULT is set) :
                // SQL_CDO_CONSTRAINT_INITIALLY_DEFERRED (Full level)
                // SQL_CDO_CONSTRAINT_INITIALLY_IMMEDIATE (Full level)
                // SQL_CDO_CONSTRAINT_DEFERRABLE (Full level)
                // SQL_CDO_CONSTRAINT_NON_DEFERRABLE (Full level)
                //
                // A return value of "0" means that the CREATE DOMAIN statement is not supported.
                intParams[SQL_CREATE_DOMAIN] = 0;
#endif // SQL_CREATE_DOMAIN

#ifdef SQL_CREATE_SCHEMA
                // Bitmask enumerating the clauses in the CREATE SCHEMA statement, as defined in SQL-92, supported by
                // the data source.
                // The following bitmasks are used to determine which clauses are supported :
                // SQL_CS_CREATE_SCHEMA
                // SQL_CS_AUTHORIZATION
                // SQL_CS_DEFAULT_CHARACTER_SET
                //
                // An SQL - 92 Intermediate level-conformant driver will always return the SQL_CS_CREATE_SCHEMA and
                // SQL_CS_AUTHORIZATION options as supported. These must also be supported at the SQL-92 Entry level,
                // but not necessarily as SQL statements. An SQL-92 Full level-conformant driver will always return all
                // of these options as supported.
                intParams[SQL_CREATE_SCHEMA] = 0;
#endif // SQL_CREATE_SCHEMA

#ifdef SQL_CREATE_TABLE
                // Bitmask enumerating the clauses in the CREATE TABLE statement, as defined in SQL-92, supported by
                // the data source.
                // The SQL - 92 or FIPS conformance level at which this feature must be supported is shown in
                // parentheses next to each bitmask.
                //
                // The following bitmasks are used to determine which clauses are supported :
                // SQL_CT_CREATE_TABLE = The CREATE TABLE statement is supported. (Entry level)
                // SQL_CT_TABLE_CONSTRAINT = Specifying table constraints is supported (FIPS Transitional level)
                // SQL_CT_CONSTRAINT_NAME_DEFINITION = The <constraint name definition> clause is supported for naming
                //     column and table constraints (Intermediate level)
                //
                // The following bits specify the ability to create temporary tables :
                // SQL_CT_COMMIT_PRESERVE = Deleted rows are preserved on commit. (Full level)
                // SQL_CT_COMMIT_DELETE = Deleted rows are deleted on commit. (Full level)
                // SQL_CT_GLOBAL_TEMPORARY = Global temporary tables can be created. (Full level)
                // SQL_CT_LOCAL_TEMPORARY = Local temporary tables can be created. (Full level)
                //
                // The following bits specify the ability to create column constraints :
                // SQL_CT_COLUMN_CONSTRAINT = Specifying column constraints is supported (FIPS Transitional level)
                // SQL_CT_COLUMN_DEFAULT = Specifying column defaults is supported (FIPS Transitional level)
                // SQL_CT_COLUMN_COLLATION = Specifying column collation is supported (Full level)
                //
                // The following bits specify the supported constraint attributes if specifying column or table
                // constraints is supported :
                // SQL_CT_CONSTRAINT_INITIALLY_DEFERRED (Full level)
                // SQL_CT_CONSTRAINT_INITIALLY_IMMEDIATE (Full level)
                // SQL_CT_CONSTRAINT_DEFERRABLE (Full level)
                // SQL_CT_CONSTRAINT_NON_DEFERRABLE (Full level)
                intParams[SQL_CREATE_TABLE] = SQL_CT_CREATE_TABLE | SQL_CT_COLUMN_CONSTRAINT;
#endif // SQL_CREATE_TABLE

#ifdef SQL_CREATE_TRANSLATION
                // Bitmask enumerating the clauses in the CREATE TRANSLATION statement, as defined in SQL-92, supported
                // by the data source.
                //
                // The following bitmask is used to determine which clauses are supported :
                // SQL_CTR_CREATE_TRANSLATION
                //
                // An SQL - 92 Full level-conformant driver will always return these options as supported. A return
                // value of "0" means that the CREATE TRANSLATION statement is not supported.
                intParams[SQL_CREATE_TRANSLATION] = 0;
#endif // SQL_CREATE_TRANSLATION

#ifdef SQL_CREATE_VIEW
                // Bitmask enumerating the clauses in the CREATE VIEW statement, as defined in SQL-92, supported by the
                // data source.
                //
                // The following bitmasks are used to determine which clauses are supported :
                // SQL_CV_CREATE_VIEW
                // SQL_CV_CHECK_OPTION
                // SQL_CV_CASCADEDSQL_CV_LOCAL
                //
                // A return value of "0" means that the CREATE VIEW statement is not supported.
                // An SQL - 92 Entry level-conformant driver will always return the SQL_CV_CREATE_VIEW and
                // SQL_CV_CHECK_OPTION options as supported.
                // An SQL - 92 Full level-conformant driver will always return all of these options as supported.
                intParams[SQL_CREATE_VIEW] = 0;
#endif // SQL_CREATE_VIEW

#ifdef SQL_CURSOR_SENSITIVITY
                // Value that indicates the support for cursor sensitivity:
                // SQL_INSENSITIVE = All cursors on the statement handle show the result set without reflecting any
                //     changes that were made to it by any other cursor within the same transaction.
                // SQL_UNSPECIFIED = It is unspecified whether cursors on the statement handle make visible the changes
                //     that were made to a result set by another cursor within the same transaction. Cursors on the
                //     statement handle may make visible none, some, or all such changes.
                // SQL_SENSITIVE = Cursors are sensitive to changes that were made by other cursors within the same
                //     transaction.
                //
                // An SQL - 92 Entry level-conformant driver will always return the SQL_UNSPECIFIED option as supported.
                // An SQL - 92 Full level-conformant driver will always return the SQL_INSENSITIVE option as supported.
                intParams[SQL_CURSOR_SENSITIVITY] = SQL_INSENSITIVE;
#endif // SQL_CURSOR_SENSITIVITY

#ifdef SQL_DDL_INDEX
                // Value that indicates support for creation and dropping of indexes:
                // SQL_DI_CREATE_INDEX
                // SQL_DI_DROP_INDEX
                intParams[SQL_DDL_INDEX] = SQL_DI_CREATE_INDEX | SQL_DI_DROP_INDEX;
#endif // SQL_DDL_INDEX

#ifdef SQL_DEFAULT_TXN_ISOLATION
                // Value that indicates the default transaction isolation level supported by the driver or data source,
                // or zero if the data source does not support transactions. The following terms are used to define
                // transaction isolation levels:
                //
                // Dirty Read Transaction 1 changes a row. Transaction 2 reads the changed row before transaction 1
                // commits the change. If transaction 1 rolls back the change, transaction 2 will have read a row that
                // is considered to have never existed.
                // Nonrepeatable Read Transaction 1 reads a row.Transaction 2 updates or deletes that row and commits
                // this change. If transaction 1 tries to reread the row, it will receive different row values or
                // discover that the row has been deleted.
                // Phantom Transaction 1 reads a set of rows that satisfy some search criteria. Transaction 2 generates
                // one or more rows (through either inserts or updates) that match the search criteria. If transaction 1
                // reexecutes the statement that reads the rows, it receives a different set of rows.
                //
                // If the data source supports transactions, the driver returns one of the following bitmasks :
                // SQL_TXN_READ_UNCOMMITTED = Dirty reads, nonrepeatable reads, and phantoms are possible.
                // SQL_TXN_READ_COMMITTED = Dirty reads are not possible. Nonrepeatable reads and phantoms are possible
                // SQL_TXN_REPEATABLE_READ = Dirty reads and nonrepeatable reads are not possible. Phantoms are possible
                // SQL_TXN_SERIALIZABLE = Transactions are serializable. Serializable transactions do not allow dirty
                //     reads, nonrepeatable reads, or phantoms.
                intParams[SQL_DEFAULT_TXN_ISOLATION] = SQL_TXN_REPEATABLE_READ;
#endif // SQL_DEFAULT_TXN_ISOLATION

#ifdef SQL_DROP_ASSERTION
                // A bitmask enumerating the clauses in the DROP ASSERTION statement, as defined in SQL-92,
                // supported by the data source.
                // The following bitmask is used to determine which clauses are supported :
                // SQL_DA_DROP_ASSERTION
                // An SQL-92 Full level-conformant driver will always return this option as supported.
                intParams[SQL_DROP_ASSERTION] = 0;
#endif // SQL_DROP_ASSERTION

#ifdef SQL_DROP_CHARACTER_SET
                // A bitmask enumerating the clauses in the DROP CHARACTER SET statement, as defined in
                // SQL-92, supported by the data source.
                // The following bitmask is used to determine which clauses are supported :
                // SQL_DCS_DROP_CHARACTER_SET
                // An SQL-92 Full level-conformant driver will always return this option as supported.
                intParams[SQL_DROP_CHARACTER_SET] = 0;
#endif // SQL_DROP_CHARACTER_SET

#ifdef SQL_DROP_COLLATION
                // A bitmask enumerating the clauses in the DROP COLLATION statement, as defined in SQL-92, supported by
                // the data source.
                // The following bitmask is used to determine which clauses are supported :
                // SQL_DC_DROP_COLLATION
                // An SQL-92 Full level-conformant driver will always return this option as supported.
                intParams[SQL_DROP_COLLATION] = 0;
#endif // SQL_DROP_COLLATION

#ifdef SQL_DROP_DOMAIN
                // A bitmask enumerating the clauses in the DROP DOMAIN statement, as defined in SQL-92, supported by
                // the data source.
                // The following bitmasks are used to determine which clauses are supported :
                // SQL_DD_DROP_DOMAIN
                // SQL_DD_CASCADE
                // SQL_DD_RESTRICT
                // An SQL-92 Intermediate level-conformant driver will always return all of these options as supported.
                intParams[SQL_DROP_DOMAIN] = 0;
#endif // SQL_DROP_DOMAIN

#ifdef SQL_DROP_SCHEMA
                // A bitmask enumerating the clauses in the DROP SCHEMA statement, as defined in SQL-92, supported by
                // the data source.
                // The following bitmasks are used to determine which clauses are supported :
                // SQL_DS_DROP_SCHEMA
                // SQL_DS_CASCADE
                // SQL_DS_RESTRICT
                // An SQL-92 Intermediate level-conformant driver will always return all of these options as supported.
                intParams[SQL_DROP_SCHEMA] = 0;
#endif // SQL_DROP_SCHEMA

#ifdef SQL_DROP_TABLE
                // A bitmask enumerating the clauses in the DROP TABLE statement, as defined in SQL-92, supported by the
                // data source.
                // The following bitmasks are used to determine which clauses are supported:
                // SQL_DT_DROP_TABLE
                // SQL_DT_CASCADE
                // SQL_DT_RESTRICT
                // An FIPS Transitional level-conformant driver will always return all of these options as supported.
                intParams[SQL_DROP_TABLE] = SQL_DT_DROP_TABLE;
#endif // SQL_DROP_TABLE

#ifdef SQL_DROP_TRANSLATION
                // A bitmask enumerating the clauses in the DROP TRANSLATION statement, as defined in SQL-92, supported
                // by the data source.
                // The following bitmask is used to determine which clauses are supported:
                // SQL_DTR_DROP_TRANSLATION
                // An SQL-92 Full level-conformant driver will always return this option as supported.
                intParams[SQL_DROP_TRANSLATION] = 0;
#endif // SQL_DROP_TRANSLATION

#ifdef SQL_DROP_VIEW
                // A bitmask enumerating the clauses in the DROP VIEW statement, as defined in SQL-92, supported by the
                // data source.
                // The following bitmasks are used to determine which clauses are supported:
                // SQL_DV_DROP_VIEW
                // SQL_DV_CASCADE
                // SQL_DV_RESTRICT
                // An FIPS Transitional level-conformant driver will always return all of these options as supported.
                intParams[SQL_DROP_VIEW] = 0;
#endif // SQL_DROP_VIEW

#ifdef SQL_DYNAMIC_CURSOR_ATTRIBUTES1
                // A bitmask that describes the attributes of a dynamic cursor that are supported by the driver.
                // This bitmask contains the first subset of attributes; for the second subset, see
                // SQL_DYNAMIC_CURSOR_ATTRIBUTES2.
                //
                // The following bitmasks are used to determine which attributes are supported:
                // SQL_CA1_NEXT = A FetchOrientation argument of SQL_FETCH_NEXT is supported in a call to SQLFetchScroll
                //     when the cursor is a dynamic cursor.
                // SQL_CA1_ABSOLUTE = FetchOrientation arguments of SQL_FETCH_FIRST, SQL_FETCH_LAST, and
                //     SQL_FETCH_ABSOLUTE are supported in a call to SQLFetchScroll when the cursor is a dynamic cursor.
                //     (The rowset that will be fetched is independent of the current cursor position.)
                // SQL_CA1_RELATIVE = FetchOrientation arguments of SQL_FETCH_PRIOR and SQL_FETCH_RELATIVE are supported
                //     in a call to SQLFetchScroll when the cursor is a dynamic cursor. (The rowset that will be fetched
                //     depends on the current cursor position. Note that this is separated from SQL_FETCH_NEXT because
                //     in a forward-only cursor, only SQL_FETCH_NEXT is supported.)
                // SQL_CA1_BOOKMARK = A FetchOrientation argument of SQL_FETCH_BOOKMARK is supported in a call to
                //     SQLFetchScroll when the cursor is a dynamic cursor.
                // SQL_CA1_LOCK_EXCLUSIVE = A LockType argument of SQL_LOCK_EXCLUSIVE is supported in a call to
                //     SQLSetPos when the cursor is a dynamic cursor.
                // SQL_CA1_LOCK_NO_CHANGE = A LockType argument of SQL_LOCK_NO_CHANGE is supported in a call to
                //     SQLSetPos when the cursor is a dynamic cursor.
                // SQL_CA1_LOCK_UNLOCK = A LockType argument of SQL_LOCK_UNLOCK is supported in a call to SQLSetPos
                //     when the cursor is a dynamic cursor.
                // SQL_CA1_POS_POSITION = An Operation argument of SQL_POSITION is supported in a call to SQLSetPos when
                //     the cursor is a dynamic cursor.
                // SQL_CA1_POS_UPDATE = An Operation argument of SQL_UPDATE is supported in a call to SQLSetPos when the
                //     cursor is a dynamic cursor.
                // SQL_CA1_POS_DELETE = An Operation argument of SQL_DELETE is supported in a call to SQLSetPos when the
                //     cursor is a dynamic cursor.
                // SQL_CA1_POS_REFRESH = An Operation argument of SQL_REFRESH is supported in a call to SQLSetPos when
                //     the cursor is a dynamic cursor.
                // SQL_CA1_POSITIONED_UPDATE = An UPDATE WHERE CURRENT OF SQL statement is supported when the cursor is
                //     a dynamic cursor. (An SQL-92 Entry level-conformant driver will always return this option as
                //     supported.)
                // SQL_CA1_POSITIONED_DELETE = A DELETE WHERE CURRENT OF SQL statement is supported when the cursor is a
                //     dynamic cursor. (An SQL-92 Entry level-conformant driver will always return this option as
                //     supported.)
                // SQL_CA1_SELECT_FOR_UPDATE = A SELECT FOR UPDATE SQL statement is supported when the cursor is a
                //     dynamic cursor. (An SQL-92 Entry level-conformant driver will always return this option as
                //     supported.)
                // SQL_CA1_BULK_ADD = An Operation argument of SQL_ADD is supported in a call to SQLBulkOperations when
                //     the cursor is a dynamic cursor.
                // SQL_CA1_BULK_UPDATE_BY_BOOKMARK = An Operation argument of SQL_UPDATE_BY_BOOKMARK is supported in a
                //     call to SQLBulkOperations when the cursor is a dynamic cursor.
                // SQL_CA1_BULK_DELETE_BY_BOOKMARK = An Operation argument of SQL_DELETE_BY_BOOKMARK is supported in a
                //     call to SQLBulkOperations when the cursor is a dynamic cursor.
                // SQL_CA1_BULK_FETCH_BY_BOOKMARK = An Operation argument of SQL_FETCH_BY_BOOKMARK is supported in a
                //     call to SQLBulkOperations when the cursor is a dynamic cursor.
                //
                // An SQL-92 Intermediate level-conformant driver will usually return the SQL_CA1_NEXT,
                // SQL_CA1_ABSOLUTE, and SQL_CA1_RELATIVE options as supported, because it supports scrollable cursors
                // through the embedded SQL FETCH statement. Because this does not directly determine the underlying SQL
                // support, however, scrollable cursors may not be supported, even for an SQL-92 Intermediate
                // level-conformant driver.
                intParams[SQL_DYNAMIC_CURSOR_ATTRIBUTES1] = SQL_CA1_NEXT;
#endif // SQL_DYNAMIC_CURSOR_ATTRIBUTES1

#ifdef SQL_DYNAMIC_CURSOR_ATTRIBUTES2
                // A bitmask that describes the attributes of a dynamic cursor that are supported by the driver.
                // This bitmask contains the second subset of attributes; for the first subset, see
                // SQL_DYNAMIC_CURSOR_ATTRIBUTES1.
                //
                // The following bitmasks are used to determine which attributes are supported:
                // SQL_CA2_READ_ONLY_CONCURRENCY = A read-only dynamic cursor, in which no updates are allowed, is
                //     supported. (The SQL_ATTR_CONCURRENCY statement attribute can be SQL_CONCUR_READ_ONLY for a
                //     dynamic cursor).
                // SQL_CA2_LOCK_CONCURRENCY = A dynamic cursor that uses the lowest level of locking sufficient to make
                //     sure that the row can be updated is supported. (The SQL_ATTR_CONCURRENCY statement attribute can
                //     be SQL_CONCUR_LOCK for a dynamic cursor.) These locks must be consistent with the transaction
                //     isolation level set by the SQL_ATTR_TXN_ISOLATION connection attribute.
                // SQL_CA2_OPT_ROWVER_CONCURRENCY = A dynamic cursor that uses the optimistic concurrency control
                //     comparing row versions is supported. (The SQL_ATTR_CONCURRENCY statement attribute can be
                //     SQL_CONCUR_ROWVER for a dynamic cursor.)
                // SQL_CA2_OPT_VALUES_CONCURRENCY = A dynamic cursor that uses the optimistic concurrency control
                //     comparing values is supported. (The SQL_ATTR_CONCURRENCY statement attribute can be
                //     SQL_CONCUR_VALUES for a dynamic cursor.)
                // SQL_CA2_SENSITIVITY_ADDITIONS = Added rows are visible to a dynamic cursor; the cursor can scroll to
                //     those rows. (Where these rows are added to the cursor is driver-dependent.)
                // SQL_CA2_SENSITIVITY_DELETIONS = Deleted rows are no longer available to a dynamic cursor, and do not
                //     leave a "hole" in the result set; after the dynamic cursor scrolls from a deleted row, it cannot
                //     return to that row.
                // SQL_CA2_SENSITIVITY_UPDATES = Updates to rows are visible to a dynamic cursor; if the dynamic cursor
                //     scrolls from and returns to an updated row, the data returned by the cursor is the updated data,
                //     not the original data.
                // SQL_CA2_MAX_ROWS_SELECT = The SQL_ATTR_MAX_ROWS statement attribute affects SELECT statements when
                //     the cursor is a dynamic cursor.
                // SQL_CA2_MAX_ROWS_INSERT = The SQL_ATTR_MAX_ROWS statement attribute affects INSERT statements when
                //     the cursor is a dynamic cursor.
                // SQL_CA2_MAX_ROWS_DELETE = The SQL_ATTR_MAX_ROWS statement attribute affects DELETE statements when
                //     the cursor is a dynamic cursor.
                // SQL_CA2_MAX_ROWS_UPDATE = The SQL_ATTR_MAX_ROWS statement attribute affects UPDATE statements when
                //     the cursor is a dynamic cursor.
                // SQL_CA2_MAX_ROWS_CATALOG = The SQL_ATTR_MAX_ROWS statement attribute affects CATALOG result sets when
                //     the cursor is a dynamic cursor.
                // SQL_CA2_MAX_ROWS_AFFECTS_ALL = The SQL_ATTR_MAX_ROWS statement attribute affects SELECT, INSERT,
                //     DELETE, and UPDATE statements, and CATALOG result sets, when the cursor is a dynamic cursor.
                // SQL_CA2_CRC_EXACT = The exact row count is available in the SQL_DIAG_CURSOR_ROW_COUNT diagnostic
                //     field when the cursor is a dynamic cursor.
                // SQL_CA2_CRC_APPROXIMATE = An approximate row count is available in the SQL_DIAG_CURSOR_ROW_COUNT
                //     diagnostic field when the cursor is a dynamic cursor.
                // SQL_CA2_SIMULATE_NON_UNIQUE = The driver does not guarantee that simulated positioned update or
                //     delete statements will affect only one row when the cursor is a dynamic cursor; it is the
                //     application's responsibility to guarantee this. (If a statement affects more than one row,
                //     SQLExecute or SQLExecDirect returns SQLSTATE 01001 [Cursor operation conflict].) To set this
                //     behavior, the application calls SQLSetStmtAttr with the SQL_ATTR_SIMULATE_CURSOR attribute set to
                //     SQL_SC_NON_UNIQUE.
                // SQL_CA2_SIMULATE_TRY_UNIQUE = The driver tries to guarantee that simulated positioned update or
                //     delete statements will affect only one row when the cursor is a dynamic cursor. The driver always
                //     executes such statements, even if they might affect more than one row, such as when there is no
                //     unique key. (If a statement affects more than one row, SQLExecute or SQLExecDirect returns
                //     SQLSTATE 01001 [Cursor operation conflict].) To set this behavior, the application calls
                //     SQLSetStmtAttr with the SQL_ATTR_SIMULATE_CURSOR attribute set to SQL_SC_TRY_UNIQUE.
                // SQL_CA2_SIMULATE_UNIQUE = The driver guarantees that simulated positioned update or delete statements
                //     will affect only one row when the cursor is a dynamic cursor. If the driver cannot guarantee this
                //     for a given statement, SQLExecDirect or SQLPrepare return SQLSTATE 01001 (Cursor operation
                //     conflict). To set this behavior, the application calls SQLSetStmtAttr with the
                //     SQL_ATTR_SIMULATE_CURSOR attribute set to SQL_SC_UNIQUE.
                intParams[SQL_DYNAMIC_CURSOR_ATTRIBUTES2] = SQL_CA2_READ_ONLY_CONCURRENCY;
#endif // SQL_DYNAMIC_CURSOR_ATTRIBUTES2

#ifdef SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1
                // A bitmask that describes the attributes of a forward-only cursor that are supported by the driver.
                // This bitmask contains the first subset of attributes; for the second subset, see
                // SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2.
                //
                // The following bitmasks are used to determine which attributes are supported:
                // SQL_CA1_NEXT
                // SQL_CA1_LOCK_EXCLUSIVE
                // SQL_CA1_LOCK_NO_CHANGE
                // SQL_CA1_LOCK_UNLOCK
                // SQL_CA1_POS_POSITION
                // SQL_CA1_POS_UPDATE
                // SQL_CA1_POS_DELETE
                // SQL_CA1_POS_REFRESH
                // SQL_CA1_POSITIONED_UPDATE
                // SQL_CA1_POSITIONED_DELETE
                // SQL_CA1_SELECT_FOR_UPDATE
                // SQL_CA1_BULK_ADD
                // SQL_CA1_BULK_UPDATE_BY_BOOKMARK
                // SQL_CA1_BULK_DELETE_BY_BOOKMARK
                // SQL_CA1_BULK_FETCH_BY_BOOKMARK
                //
                // For descriptions of these bitmasks, see SQL_DYNAMIC_CURSOR_ATTRIBUTES1 (and substitute "forward-only
                // cursor" for "dynamic cursor" in the descriptions).
                intParams[SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1] = SQL_CA1_NEXT;
#endif // SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1

#ifdef SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2
                // A bitmask that describes the attributes of a forward-only cursor that are supported by the driver.
                // This bitmask contains the second subset of attributes; for the first subset, see
                // SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1.
                //
                // The following bitmasks are used to determine which attributes are supported:
                // SQL_CA2_READ_ONLY_CONCURRENCY
                // SQL_CA2_LOCK_CONCURRENCY
                // SQL_CA2_OPT_ROWVER_CONCURRENCY
                // SQL_CA2_OPT_VALUES_CONCURRENCY
                // SQL_CA2_SENSITIVITY_ADDITIONS
                // SQL_CA2_SENSITIVITY_DELETIONS
                // SQL_CA2_SENSITIVITY_UPDATES
                // SQL_CA2_MAX_ROWS_SELECT
                // SQL_CA2_MAX_ROWS_INSERT
                // SQL_CA2_MAX_ROWS_DELETE
                // SQL_CA2_MAX_ROWS_UPDATE
                // SQL_CA2_MAX_ROWS_CATALOG
                // SQL_CA2_MAX_ROWS_AFFECTS_ALL
                // SQL_CA2_CRC_EXACT
                // SQL_CA2_CRC_APPROXIMATE
                // SQL_CA2_SIMULATE_NON_UNIQUE
                // SQL_CA2_SIMULATE_TRY_UNIQUE
                // SQL_CA2_SIMULATE_UNIQUE
                //
                // For descriptions of these bitmasks, see SQL_DYNAMIC_CURSOR_ATTRIBUTES2 (and substitute "forward-only
                // cursor" for "dynamic cursor" in the descriptions).
                intParams[SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2] = SQL_CA2_READ_ONLY_CONCURRENCY;
#endif // SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2

#ifdef SQL_INDEX_KEYWORDS
                // A bitmask that enumerates keywords in the CREATE INDEX statement that are supported by the driver:
                //
                // SQL_IK_NONE = None of the keywords is supported.
                // SQL_IK_ASC = ASC keyword is supported.
                // SQL_IK_DESC = DESC keyword is supported.
                // SQL_IK_ALL = All keywords are supported.
                //
                // To see whether the CREATE INDEX statement is supported, an application calls SQLGetInfo with the
                // SQL_DLL_INDEX information type.
                intParams[SQL_INDEX_KEYWORDS] = SQL_IK_ALL;
#endif // SQL_INDEX_KEYWORDS

#ifdef SQL_INFO_SCHEMA_VIEWS
                // A bitmask enumerating the views in the INFORMATION_SCHEMA that are supported by the driver. The views
                // in, and the contents of, INFORMATION_SCHEMA are as defined in SQL-92. The SQL-92 or FIPS conformance
                // level at which this feature must be supported is shown in parentheses next to each bitmask.
                //
                // The following bitmasks are used to determine which views are supported:
                // SQL_ISV_ASSERTIONS = Identifies the catalog's assertions that are owned by a given user. (Full level)
                // SQL_ISV_CHARACTER_SETS = Identifies the catalog's character sets that can be accessed by a given
                //     user. (Intermediate level)
                // SQL_ISV_CHECK_CONSTRAINTS = Identifies the CHECK constraints that are owned by a given user.
                //     (Intermediate level)
                // SQL_ISV_COLLATIONS = Identifies the character collations for the catalog that can be accessed by a
                //     given user. (Full level)
                // SQL_ISV_COLUMN_DOMAIN_USAGE = Identifies columns for the catalog that depend on domains defined in
                //     the catalog and are owned by a given user. (Intermediate level)
                // SQL_ISV_COLUMN_PRIVILEGES = Identifies the privileges on columns of persistent tables that are
                //     available to or granted by a given user. (FIPS Transitional level)
                // SQL_ISV_COLUMNS = Identifies the columns of persistent tables that can be accessed by a given user.
                //     (FIPS Transitional level)
                // SQL_ISV_CONSTRAINT_COLUMN_USAGE = Similar to CONSTRAINT_TABLE_USAGE view, columns are identified for
                //     the various constraints that are owned by a given user. (Intermediate level)
                // SQL_ISV_CONSTRAINT_TABLE_USAGE = Identifies the tables that are used by constraints (referential,
                //     unique, and assertions), and are owned by a given user. (Intermediate level)
                // SQL_ISV_DOMAIN_CONSTRAINTS = Identifies the domain constraints (of the domains in the catalog) that
                //     can be accessed by a given user. (Intermediate level)
                // SQL_ISV_DOMAINS = Identifies the domains defined in a catalog that can be accessed by the user.
                //     (Intermediate level)
                // SQL_ISV_KEY_COLUMN_USAGE = Identifies columns defined in the catalog that are constrained as keys
                //     by a given user. (Intermediate level)
                // SQL_ISV_REFERENTIAL_CONSTRAINTS = Identifies the referential constraints that are owned by a given
                //     user. (Intermediate level)
                // SQL_ISV_SCHEMATA = Identifies the schemas that are owned by a given user. (Intermediate level)
                // SQL_ISV_SQL_LANGUAGES = Identifies the SQL conformance levels, options, and dialects supported by
                //     the SQL implementation. (Intermediate level)
                // SQL_ISV_TABLE_CONSTRAINTS = Identifies the table constraints that are owned by a given user.
                //     (Intermediate level)
                // SQL_ISV_TABLE_PRIVILEGES = Identifies the privileges on persistent tables that are available to or
                //     granted by a given user. (FIPS Transitional level)
                // SQL_ISV_TABLES = Identifies the persistent tables defined in a catalog that can be accessed by a
                //     given user. (FIPS Transitional level)
                // SQL_ISV_TRANSLATIONS = Identifies character translations for the catalog that can be accessed by a
                //     given user. (Full level)
                // SQL_ISV_USAGE_PRIVILEGES = Identifies the USAGE privileges on catalog objects that are available to
                //     or owned by a given user. (FIPS Transitional level)
                // SQL_ISV_VIEW_COLUMN_USAGE = Identifies the columns on which the catalog's views that are owned by a
                //     given user are dependent. (Intermediate level)
                // SQL_ISV_VIEW_TABLE_USAGE = Identifies the tables on which the catalog's views that are owned by a
                //     given user are dependent. (Intermediate level)
                // SQL_ISV_VIEWS = Identifies the viewed tables defined in this catalog that can be accessed by a given
                //     user. (FIPS Transitional level)
                intParams[SQL_INFO_SCHEMA_VIEWS] = 0;
#endif // SQL_INFO_SCHEMA_VIEWS

#ifdef SQL_INSERT_STATEMENT
                // A bitmask that indicates support for INSERT statements:
                // SQL_IS_INSERT_LITERALS
                // SQL_IS_INSERT_SEARCHED
                // SQL_IS_SELECT_INTO
                // An SQL-92 Entry level-conformant driver will always return all of these options as supported.
                intParams[SQL_INSERT_STATEMENT] = SQL_IS_INSERT_LITERALS | SQL_IS_INSERT_SEARCHED;
#endif // SQL_INSERT_STATEMENT

#ifdef SQL_KEYSET_CURSOR_ATTRIBUTES1
                // A bitmask that describes the attributes of a keyset cursor that are supported by the driver.
                // This bitmask contains the first subset of attributes; for the second subset, see
                // SQL_KEYSET_CURSOR_ATTRIBUTES2.
                //
                // The following bitmasks are used to determine which attributes are supported:
                // SQL_CA1_NEXT
                // SQL_CA1_ABSOLUTE
                // SQL_CA1_RELATIVE
                // SQL_CA1_BOOKMARK
                // SQL_CA1_LOCK_EXCLUSIVE
                // SQL_CA1_LOCK_NO_CHANGE
                // SQL_CA1_LOCK_UNLOCK
                // SQL_CA1_POS_POSITION
                // SQL_CA1_POS_UPDATE
                // SQL_CA1_POS_DELETE
                // SQL_CA1_POS_REFRESH
                // SQL_CA1_POSITIONED_UPDATE
                // SQL_CA1_POSITIONED_DELETE
                // SQL_CA1_SELECT_FOR_UPDATE
                // SQL_CA1_BULK_ADD
                // SQL_CA1_BULK_UPDATE_BY_BOOKMARK
                // SQL_CA1_BULK_DELETE_BY_BOOKMARK
                // SQL_CA1_BULK_FETCH_BY_BOOKMARK
                //
                // For descriptions of these bitmasks, see SQL_DYNAMIC_CURSOR_ATTRIBUTES1 (and substitute "keyset-driven
                // cursor" for "dynamic cursor" in the descriptions).
                //
                // An SQL-92 Intermediate level-conformant driver will usually return the SQL_CA1_NEXT,
                // SQL_CA1_ABSOLUTE, and SQL_CA1_RELATIVE options as supported, because the driver supports scrollable
                // cursors through the embedded SQL FETCH statement. Because this does not directly determine the
                // underlying SQL support, however, scrollable cursors may not be supported, even for an SQL-92
                // Intermediate level-conformant driver.
                intParams[SQL_KEYSET_CURSOR_ATTRIBUTES1] = SQL_CA1_NEXT;
#endif // SQL_KEYSET_CURSOR_ATTRIBUTES1

#ifdef SQL_KEYSET_CURSOR_ATTRIBUTES2
                // A bitmask that describes the attributes of a keyset cursor that are supported by the driver.
                // This bitmask contains the second subset of attributes; for the first subset, see
                // SQL_KEYSET_CURSOR_ATTRIBUTES1.
                //
                // The following bitmasks are used to determine which attributes are supported:
                // SQL_CA2_READ_ONLY_CONCURRENCY
                // SQL_CA2_LOCK_CONCURRENCY
                // SQL_CA2_OPT_ROWVER_CONCURRENCY
                // SQL_CA2_OPT_VALUES_CONCURRENCY
                // SQL_CA2_SENSITIVITY_ADDITIONS
                // SQL_CA2_SENSITIVITY_DELETIONS
                // SQL_CA2_SENSITIVITY_UPDATES
                // SQL_CA2_MAX_ROWS_SELECT
                // SQL_CA2_MAX_ROWS_INSERT
                // SQL_CA2_MAX_ROWS_DELETE
                // SQL_CA2_MAX_ROWS_UPDATE
                // SQL_CA2_MAX_ROWS_CATALOG
                // SQL_CA2_MAX_ROWS_AFFECTS_ALL
                // SQL_CA2_CRC_EXACTSQL_CA2_CRC_APPROXIMATE
                // SQL_CA2_SIMULATE_NON_UNIQUE
                // SQL_CA2_SIMULATE_TRY_UNIQUE
                // SQL_CA2_SIMULATE_UNIQUE
                //
                // For descriptions of these bitmasks, see SQL_DYNAMIC_CURSOR_ATTRIBUTES1 (and substitute "keyset-driven
                // cursor" for "dynamic cursor" in the descriptions).
                intParams[SQL_KEYSET_CURSOR_ATTRIBUTES2] = 0;
#endif // SQL_KEYSET_CURSOR_ATTRIBUTES2

#ifdef SQL_MAX_ASYNC_CONCURRENT_STATEMENTS
                // Value that specifies the maximum number of active concurrent statements in asynchronous mode that the
                // driver can support on a given connection. If there is no specific limit or the limit is unknown, this
                // value is zero.
                intParams[SQL_MAX_ASYNC_CONCURRENT_STATEMENTS] = 0;
#endif // SQL_MAX_ASYNC_CONCURRENT_STATEMENTS

#ifdef SQL_MAX_BINARY_LITERAL_LEN
                // Value that specifies the maximum length (number of hexadecimal characters, excluding the literal
                // prefix and suffix returned by SQLGetTypeInfo) of a binary literal in an SQL statement. For example,
                // the binary literal 0xFFAA has a length of 4. If there is no maximum length or the length is unknown,
                // this value is set to zero.
                intParams[SQL_MAX_BINARY_LITERAL_LEN] = 0;
#endif // SQL_MAX_BINARY_LITERAL_LEN

#ifdef SQL_MAX_CATALOG_NAME_LEN
                // Value that specifies the maximum length of a catalog name in the data source. If there is no maximum
                // length or the length is unknown, this value is set to zero.
                // An FIPS Full level-conformant driver will return at least 128.
                // This InfoType has been renamed for ODBC 3.0 from the ODBC 2.0 InfoType SQL_MAX_QUALIFIER_NAME_LEN.
                intParams[SQL_MAX_CATALOG_NAME_LEN] = 0;
#endif // SQL_MAX_CATALOG_NAME_LEN

#ifdef SQL_MAX_CHAR_LITERAL_LEN
                // Value that specifies the maximum length (number of characters, excluding the literal prefix and
                // suffix returned by SQLGetTypeInfo) of a character literal in an SQL statement. If there is no maximum
                // length or the length is unknown, this value is set to zero.
                intParams[SQL_MAX_CHAR_LITERAL_LEN] = 0;
#endif // SQL_MAX_CHAR_LITERAL_LEN

#ifdef SQL_MAX_INDEX_SIZE
                // Value that specifies the maximum number of bytes allowed in the combined fields of an index.
                // If there is no specified limit or the limit is unknown, this value is set to zero.
                intParams[SQL_MAX_INDEX_SIZE] = 0;
#endif // SQL_MAX_INDEX_SIZE

#ifdef SQL_MAX_ROW_SIZE
                // Value that specifies the maximum length of a single row in a table. If there is no specified limit or
                // the limit is unknown, this value is set to zero.
                // An FIPS Entry level-conformant driver will return at least 2,000. An FIPS Intermediate
                // level-conformant driver will return at least 8,000.
                intParams[SQL_MAX_ROW_SIZE] = 0;
#endif // SQL_MAX_ROW_SIZE

#ifdef SQL_MAX_STATEMENT_LEN
                // Value that specifies the maximum length (number of characters, including white space) of an SQL
                // statement. If there is no maximum length or the length is unknown, this value is set to zero.
                intParams[SQL_MAX_STATEMENT_LEN] = 0;
#endif // SQL_MAX_STATEMENT_LEN

#ifdef SQL_SQL92_FOREIGN_KEY_DELETE_RULE
                // A bitmask enumerating the rules supported for a foreign key in a DELETE statement, as defined in
                // SQL-92.
                //
                // The following bitmasks are used to determine which clauses are supported by the data source:
                // SQL_SFKD_CASCADE
                // SQL_SFKD_NO_ACTION
                // SQL_SFKD_SET_DEFAULT
                // SQL_SFKD_SET_NULL
                //
                // An FIPS Transitional level-conformant driver will always return all of these options as supported.
                intParams[SQL_SQL92_FOREIGN_KEY_DELETE_RULE] = 0;
#endif // SQL_SQL92_FOREIGN_KEY_DELETE_RULE

#ifdef SQL_SQL92_FOREIGN_KEY_UPDATE_RULE
                // A bitmask enumerating the rules supported for a foreign key in an UPDATE statement, as defined in
                // SQL-92.
                //
                // The following bitmasks are used to determine which clauses are supported by the data source:
                // SQL_SFKU_CASCADE
                // SQL_SFKU_NO_ACTION
                // SQL_SFKU_SET_DEFAULT
                // SQL_SFKU_SET_NULL
                //
                // An SQL-92 Full level-conformant driver will always return all of these options as supported.
                intParams[SQL_SQL92_FOREIGN_KEY_UPDATE_RULE] = 0;
#endif // SQL_SQL92_FOREIGN_KEY_UPDATE_RULE

#ifdef SQL_SQL92_GRANT
                // A bitmask enumerating the clauses supported in the GRANT statement, as defined in SQL-92.
                // The SQL-92 or FIPS conformance level at which this feature must be supported is shown in parentheses
                // next to each bitmask.
                // The following bitmasks are used to determine which clauses are supported by the data source:
                // SQL_SG_DELETE_TABLE (Entry level)
                // SQL_SG_INSERT_COLUMN (Intermediate level)
                // SQL_SG_INSERT_TABLE (Entry level)
                // SQL_SG_REFERENCES_TABLE (Entry level)
                // SQL_SG_REFERENCES_COLUMN (Entry level)
                // SQL_SG_SELECT_TABLE (Entry level)
                // SQL_SG_UPDATE_COLUMN (Entry level)
                // SQL_SG_UPDATE_TABLE (Entry level)
                // SQL_SG_USAGE_ON_DOMAIN (FIPS Transitional level)
                // SQL_SG_USAGE_ON_CHARACTER_SET (FIPS Transitional level)
                // SQL_SG_USAGE_ON_COLLATION (FIPS Transitional level)
                // SQL_SG_USAGE_ON_TRANSLATION (FIPS Transitional level)
                // SQL_SG_WITH_GRANT_OPTION (Entry level)
                intParams[SQL_SQL92_GRANT] = 0;
#endif // SQL_SQL92_GRANT

#ifdef SQL_SQL92_REVOKE
                // A bitmask enumerating the clauses supported in the REVOKE statement, as defined in SQL-92, supported
                // by the data source.
                // The SQL-92 or FIPS conformance level at which this feature must be supported is shown in parentheses
                // next to each bitmask.
                //
                // The following bitmasks are used to determine which clauses are supported by the data source:
                // SQL_SR_CASCADE (FIPS Transitional level)
                // SQL_SR_DELETE_TABLE (Entry level)
                // SQL_SR_GRANT_OPTION_FOR (Intermediate level)
                // SQL_SR_INSERT_COLUMN (Intermediate level)
                // SQL_SR_INSERT_TABLE (Entry level)
                // SQL_SR_REFERENCES_COLUMN (Entry level)
                // SQL_SR_REFERENCES_TABLE (Entry level)
                // SQL_SR_RESTRICT (FIPS Transitional level)
                // SQL_SR_SELECT_TABLE (Entry level)
                // SQL_SR_UPDATE_COLUMN (Entry level)
                // SQL_SR_UPDATE_TABLE (Entry level)
                // SQL_SR_USAGE_ON_DOMAIN (FIPS Transitional level)
                // SQL_SR_USAGE_ON_CHARACTER_SET (FIPS Transitional level)
                // SQL_SR_USAGE_ON_COLLATION (FIPS Transitional level)
                // SQL_SR_USAGE_ON_TRANSLATION (FIPS Transitional level)
                intParams[SQL_SQL92_REVOKE] = 0;
#endif // SQL_SQL92_REVOKE

#ifdef SQL_SQL92_ROW_VALUE_CONSTRUCTOR
                // A bitmask enumerating the row value constructor expressions supported in a SELECT statement, as
                // defined in SQL-92. The following bitmasks are used to determine which options are supported by the
                // data source:
                // SQL_SRVC_VALUE_EXPRESSION
                // SQL_SRVC_NULL
                // SQL_SRVC_DEFAULT
                // SQL_SRVC_ROW_SUBQUERY
                intParams[SQL_SQL92_ROW_VALUE_CONSTRUCTOR] = SQL_SRVC_VALUE_EXPRESSION | SQL_SRVC_DEFAULT |
                    SQL_SRVC_NULL | SQL_SRVC_ROW_SUBQUERY;
#endif // SQL_SQL92_ROW_VALUE_CONSTRUCTOR

#ifdef SQL_STANDARD_CLI_CONFORMANCE
                // A bitmask enumerating the CLI standard or standards to which the driver conforms. The following
                // bitmasks are used to determine which levels the driver complies with:
                // SQL_SCC_XOPEN_CLI_VERSION1: The driver complies with the Open Group CLI version 1.
                // SQL_SCC_ISO92_CLI: The driver complies with the ISO 92 CLI.
                intParams[SQL_STANDARD_CLI_CONFORMANCE] = 0;
#endif // SQL_STANDARD_CLI_CONFORMANCE

#ifdef SQL_SUBQUERIES
                // A bitmask enumerating the predicates that support subqueries:
                //
                // SQL_SQ_CORRELATED_SUBQUERIES
                // SQL_SQ_COMPARISON
                // SQL_SQ_EXISTS
                // SQL_SQ_IN
                // SQL_SQ_QUANTIFIED
                //
                // The SQL_SQ_CORRELATED_SUBQUERIES bitmask indicates that all predicates that support subqueries
                // support correlated subqueries.
                // An SQL-92 Entry level-conformant driver will always return a bitmask in which all of these bits are
                // set.
                intParams[SQL_SUBQUERIES] = SQL_SQ_CORRELATED_SUBQUERIES | SQL_SQ_COMPARISON | SQL_SQ_EXISTS |
                    SQL_SQ_IN | SQL_SQ_QUANTIFIED;
#endif // SQL_SUBQUERIES

#ifdef SQL_TXN_ISOLATION_OPTION
                // A bitmask enumerating the transaction isolation levels available from the driver or data source.
                // The following bitmasks are used together with the flag to determine which options are supported:
                // SQL_TXN_READ_UNCOMMITTED
                // SQL_TXN_READ_COMMITTED
                // SQL_TXN_REPEATABLE_READ
                // SQL_TXN_SERIALIZABLE
                //
                // For descriptions of these isolation levels, see the description of SQL_DEFAULT_TXN_ISOLATION.
                // To set the transaction isolation level, an application calls SQLSetConnectAttr to set the
                // SQL_ATTR_TXN_ISOLATION attribute. For more information, see SQLSetConnectAttr Function.
                // An SQL-92 Entry level-conformant driver will always return SQL_TXN_SERIALIZABLE as supported.
                // A FIPS Transitional level-conformant driver will always return all of these options as supported.
                intParams[SQL_TXN_ISOLATION_OPTION] = SQL_TXN_REPEATABLE_READ;
#endif // SQL_TXN_ISOLATION_OPTION

#ifdef SQL_UNION
                // A bitmask enumerating the support for the UNION clause:
                // SQL_U_UNION = The data source supports the UNION clause.
                // SQL_U_UNION_ALL = The data source supports the ALL keyword in the UNION clause. (SQLGetInfo returns
                //     both SQL_U_UNION and SQL_U_UNION_ALL in this case.)
                // An SQL-92 Entry level-conformant driver will always return both of these options as supported.
                intParams[SQL_UNION] = SQL_U_UNION | SQL_U_UNION_ALL;
#endif // SQL_UNION

#ifdef SQL_FETCH_DIRECTION
                // DEPRECATED. Included for backward-compatibility.
                // The information type was introduced in ODBC 1.0; each bitmask is labeled with the version in which
                // it was introduced.
                // A bitmask enumerating the supported fetch direction options:
                // SQL_FD_FETCH_NEXT (ODBC 1.0)
                // SQL_FD_FETCH_FIRST (ODBC 1.0)
                // SQL_FD_FETCH_LAST (ODBC 1.0)
                // SQL_FD_FETCH_PRIOR (ODBC 1.0)
                // SQL_FD_FETCH_ABSOLUTE (ODBC 1.0)
                // SQL_FD_FETCH_RELATIVE (ODBC 1.0)
                // SQL_FD_FETCH_BOOKMARK (ODBC 2.0)
                intParams[SQL_FETCH_DIRECTION] = SQL_FD_FETCH_NEXT | SQL_FD_FETCH_PRIOR;
#endif // SQL_FETCH_DIRECTION

#ifdef SQL_LOCK_TYPES
                // DEPRECATED. Included for backward-compatibility.
                // A bitmask enumerating the supported lock types for the fLock argument in SQLSetPos:
                // SQL_LCK_NO_CHANGE
                // SQL_LCK_EXCLUSIVE
                // SQL_LCK_UNLOCK
                intParams[SQL_LOCK_TYPES] = SQL_LCK_NO_CHANGE;
#endif // SQL_LOCK_TYPES

#ifdef SQL_ODBC_API_CONFORMANCE
                // DEPRECATED. Included for backward-compatibility.
                // A value indicating the level of ODBC conformance.
                // SQL_OAC_NONE = None
                // SQL_OAC_LEVEL1 = Level 1 supported
                // SQL_OAC_LEVEL2 = Level 2 supported
                intParams[SQL_ODBC_API_CONFORMANCE] = SQL_OAC_LEVEL1;
#endif // SQL_ODBC_API_CONFORMANCE

#ifdef SQL_ODBC_SQL_CONFORMANCE
                // DEPRECATED. Included for backward-compatibility.
                // A value indicating SQL grammar supported by the driver.
                // See the following link for a definition of SQL conformance levels:
                // https://docs.microsoft.com/en-us/sql/odbc/reference/appendixes/appendix-c-sql-grammar
                //
                // SQL_OSC_MINIMUM = Minimum grammar supported
                // SQL_OSC_CORE = Core grammar supported
                // SQL_OSC_EXTENDED = Extended grammar supported
                intParams[SQL_ODBC_SQL_CONFORMANCE] = SQL_OSC_CORE;
#endif // SQL_ODBC_SQL_CONFORMANCE

#ifdef SQL_POSITIONED_STATEMENTS
                // DEPRECATED. Included for backward-compatibility.
                // A bitmask enumerating the supported positioned SQL statements.
                // The following bitmasks are used to determine which options are supported:
                // SQL_PS_POSITIONED_DELETE
                // SQL_PS_POSITIONED_UPDATE
                // SQL_PS_SELECT_FOR_UPDATE
                intParams[SQL_POSITIONED_STATEMENTS] = SQL_PS_SELECT_FOR_UPDATE;
#endif // SQL_POSITIONED_STATEMENTS

#ifdef SQL_SCROLL_CONCURRENCY
                // DEPRECATED. Included for backward-compatibility.
                // A bitmask enumerating the concurrency control options supported for the cursor.
                // The following bitmasks are used to determine which options are supported:
                // SQL_SCCO_READ_ONLY = Cursor is read-only. No updates are allowed.
                // SQL_SCCO_LOCK = Cursor uses the lowest level of locking sufficient to ensure that the row can be
                //    updated.
                // SQL_SCCO_OPT_ROWVER = Cursor uses optimistic concurrency control, comparing row versions, such as
                //    SQLBase ROWID or Sybase TIMESTAMP.
                // SQL_SCCO_OPT_VALUES = Cursor uses optimistic concurrency control, comparing values.
                intParams[SQL_SCROLL_CONCURRENCY] = SQL_SCCO_READ_ONLY;
#endif // SQL_SCROLL_CONCURRENCY

#ifdef SQL_STATIC_SENSITIVITY
                // DEPRECATED. Included for backward-compatibility.
                // A bitmask enumerating whether changes made by an application to a static or keyset-driven cursor
                // through SQLSetPos or positioned update or delete statements can be detected by that application.
                //
                // Whether an application can detect changes made to the result set by other users, including other
                // cursors in the same application, depends on the cursor type.
                //
                // SQL_SS_ADDITIONS = Added rows are visible to the cursor; the cursor can scroll to these rows.
                //    Where these rows are added to the cursor is driver-dependent.
                // SQL_SS_DELETIONS = Deleted rows are no longer available to the cursor and do not leave a "hole" in
                //   the result set; after the cursor scrolls from a deleted row, it cannot return to that row.
                // SQL_SS_UPDATES = Updates to rows are visible to the cursor; if the cursor scrolls from and returns to
                //    an updated row, the data returned by the cursor is the updated data, not the original data. This
                //    option applies only to static cursors or updates on keyset - driven cursors that do not update the
                //    key. This option does not apply for a dynamic cursor or in the case in which a key is changed in a
                //    mixed cursor.
                intParams[SQL_STATIC_SENSITIVITY] = 0;
#endif // SQL_STATIC_SENSITIVITY

                //
                //======================= Short Params ========================
                //

#ifdef SQL_MAX_CONCURRENT_ACTIVITIES
                // The maximum number of active statements that the driver can  support for a connection. Zero mean no
                // limit.
                shortParams[SQL_MAX_CONCURRENT_ACTIVITIES] = 0;
#endif // SQL_MAX_CONCURRENT_ACTIVITIES

#ifdef SQL_CURSOR_COMMIT_BEHAVIOR
                // Value that indicates how a COMMIT operation affects cursors and prepared statements in the data
                // source (the behavior of the data source when you commit a transaction).
                //
                // The value of this attribute will reflect the current state of the next setting :
                // SQL_COPT_SS_PRESERVE_CURSORS.
                // SQL_CB_DELETE = Close cursors and delete prepared statements.To use the cursor again, the application
                //     must reprepare and reexecute the statement.
                // SQL_CB_CLOSE = Close cursors. For prepared statements, the application can call SQLExecute on the
                //     statement without calling SQLPrepare again. The default for the SQL ODBC driver is SQL_CB_CLOSE.
                //     This means that the SQL ODBC driver will close your cursor(s) when you commit a transaction.
                // SQL_CB_PRESERVE = Preserve cursors in the same position as before the COMMIT operation. The
                //     application can continue to fetch data, or it can close the cursor and re-execute the statement
                //     without re-preparing it.
                // SQL_CURSOR_ROLLBACK_BEHAVIOR (ODBC 1.0)
                shortParams[SQL_CURSOR_COMMIT_BEHAVIOR] = SQL_CB_PRESERVE;
#endif // SQL_CURSOR_COMMIT_BEHAVIOR

#ifdef SQL_CURSOR_ROLLBACK_BEHAVIOR
                // Indicates how a ROLLBACK  operation affects cursors and prepared statements in the data source:
                // SQL_CB_DELETE = Close cursors and delete prepared statements. To use the cursor again, the
                //     application must reprepare and reexecute the statement.
                // SQL_CB_CLOSE = Close cursors. For prepared statements, the application can call SQLExecute on the
                //     statement without calling SQLPrepare again.
                // SQL_CB_PRESERVE = Preserve cursors in the same position as before the ROLLBACK operation. The
                //     application can continue to fetch data, or it can close the cursor and re-execute the statement
                //     without repreparing it.
                shortParams[SQL_CURSOR_ROLLBACK_BEHAVIOR] = SQL_CB_PRESERVE;
#endif // SQL_CURSOR_ROLLBACK_BEHAVIOR

#ifdef SQL_TXN_CAPABLE
                // Describs the transaction support in the driver or data source.
                shortParams[SQL_TXN_CAPABLE] = SQL_TC_DDL_COMMIT;
#endif // SQL_TXN_CAPABLE

#ifdef SQL_QUOTED_IDENTIFIER_CASE
                // Case-sensitiveness of the quoted identifiers in SQL.
                shortParams[SQL_QUOTED_IDENTIFIER_CASE] = SQL_IC_SENSITIVE;
#endif // SQL_QUOTED_IDENTIFIER_CASE

#ifdef SQL_ACTIVE_ENVIRONMENTS
                // The maximum number of active environments that the driver can support. If there is no specified limit
                // or the limit is unknown, this value is set to zero.
                shortParams[SQL_ACTIVE_ENVIRONMENTS] = 0;
#endif // SQL_ACTIVE_ENVIRONMENTS

#ifdef SQL_CONCAT_NULL_BEHAVIOR
                // Indicates how the data source handles the concatenation of NULL valued character data type columns
                // with non-NULL valued character data type columns:
                // SQL_CB_NULL = Result is NULL valued.
                // SQL_CB_NON_NULL = Result is concatenation of non - NULL valued column or columns.
                // An SQL - 92 Entry level-conformant driver will always return SQL_CB_NULL.
                shortParams[SQL_CONCAT_NULL_BEHAVIOR] = SQL_CB_NULL;
#endif // SQL_CONCAT_NULL_BEHAVIOR

#ifdef SQL_CORRELATION_NAME
                // Value that indicates whether table correlation names are supported:
                // SQL_CN_NONE = Correlation names are not supported.
                // SQL_CN_DIFFERENT = Correlation names are supported but must differ from the names of the tables they
                //     represent.
                // SQL_CN_ANY = Correlation names are supported and can be any valid user - defined name.
                // An SQL - 92 Entry level-conformant driver will always return SQL_CN_ANY.
                shortParams[SQL_CORRELATION_NAME] = SQL_CN_ANY;
#endif // SQL_CORRELATION_NAME

#ifdef SQL_FILE_USAGE
                // Value that indicates how a single-tier driver directly treats files in a data source:
                //
                // SQL_FILE_NOT_SUPPORTED = The driver is not a single-tier driver. For example, an ORACLE driver is a
                //     two-tier driver.
                // SQL_FILE_TABLE = A single-tier driver treats files in a data source as tables. For example, an Xbase
                //     driver treats each Xbase file as a table.
                // SQL_FILE_CATALOG = A single-tier driver treats files in a data source as a catalog. For example, a
                //     Microsoft Access driver treats each Microsoft Access file as a complete database.
                //
                // An application might use this to determine how users will select data. For example, Xbase users often
                // think of data as stored in files, whereas ORACLE and Microsoft Access users generally think of
                // data as stored in tables.
                //
                // When a user selects an Xbase data source, the application could display the Windows File Open common
                // dialog box; when the user selects a Microsoft Access or ORACLE data source, the application could
                // display a custom Select Table dialog box.
                shortParams[SQL_FILE_USAGE] = SQL_FILE_NOT_SUPPORTED;
#endif // SQL_FILE_USAGE

#ifdef SQL_GROUP_BY
                // Value that specifies the relationship between the columns in the GROUP BY clause and the
                // nonaggregated columns in the select list:
                //
                // SQL_GB_COLLATE = A COLLATE clause can be specified at the end of each grouping column. (ODBC 3.0)
                // SQL_GB_NOT_SUPPORTED = GROUP BY clauses are not supported. (ODBC 2.0)
                // SQL_GB_GROUP_BY_EQUALS_SELECT = The GROUP BY clause must contain all nonaggregated columns in the
                //     select list. It cannot contain any other columns.
                //     For example, SELECT DEPT, MAX(SALARY) FROM EMPLOYEE GROUP BY DEPT. (ODBC 2.0)
                // SQL_GB_GROUP_BY_CONTAINS_SELECT = The GROUP BY clause must contain all nonaggregated columns in the
                //     select list. It can contain columns that are not in the select list.
                //     For example, SELECT DEPT, MAX(SALARY) FROM EMPLOYEE GROUP BY DEPT, AGE. (ODBC 2.0)
                // SQL_GB_NO_RELATION = The columns in the GROUP BY clause and the select list are not related.
                //     The meaning of nongrouped, nonaggregated columns in the select list is data source-dependent.
                //     For example, SELECT DEPT, SALARY FROM EMPLOYEE GROUP BY DEPT, AGE. (ODBC 2.0)
                //
                // An SQL-92 Entry level-conformant driver will always return the SQL_GB_GROUP_BY_EQUALS_SELECT option
                // as supported. An SQL-92 Full level-conformant driver will always return the SQL_GB_COLLATE option as
                // supported. If none of the options is supported, the GROUP BY clause is not supported by the data
                // source.
                shortParams[SQL_GROUP_BY] = SQL_GB_GROUP_BY_EQUALS_SELECT;
#endif // SQL_GROUP_BY

#ifdef SQL_IDENTIFIER_CASE
                // Value as follows:
                //
                // SQL_IC_UPPER = Identifiers in SQL are not case-sensitive and are stored in uppercase in system
                //     catalog.
                // SQL_IC_LOWER = Identifiers in SQL are not case-sensitive and are stored in lowercase in system
                //     catalog.
                // SQL_IC_SENSITIVE = Identifiers in SQL are case sensitive and are stored in mixed case in system
                //     catalog.
                // SQL_IC_MIXED = Identifiers in SQL are not case-sensitive and are stored in mixed case in system
                //     catalog.
                //
                // Because identifiers in SQL-92 are never case-sensitive, a driver that conforms strictly to SQL-92
                // (any level) will never return the SQL_IC_SENSITIVE option as supported.
                shortParams[SQL_IDENTIFIER_CASE] = SQL_IC_UPPER;
#endif // SQL_IDENTIFIER_CASE

#ifdef SQL_MAX_COLUMN_NAME_LEN
                // Value that specifies the maximum length of a column name in the data source. If there is no maximum
                // length or the length is unknown, this value is set to zero.
                // An FIPS Entry level-conformant driver will return at least 18. An FIPS Intermediate level-conformant
                // driver will return at least 128.
                shortParams[SQL_MAX_COLUMN_NAME_LEN] = 0;
#endif // SQL_MAX_COLUMN_NAME_LEN

#ifdef SQL_MAX_COLUMNS_IN_GROUP_BY
                // Value that specifies the maximum number of columns allowed in a GROUP BY clause. If there is no
                // specified limit or the limit is unknown, this value is set to zero.
                // An FIPS Entry level-conformant driver will return at least 6. An FIPS Intermediate level-conformant
                // driver will return at least 15.
                shortParams[SQL_MAX_COLUMNS_IN_GROUP_BY] = 0;
#endif // SQL_MAX_COLUMNS_IN_GROUP_BY

#ifdef SQL_MAX_COLUMNS_IN_INDEX
                // Value that specifies the maximum number of columns allowed in an index.
                // If there is no specified limit or the limit is unknown, this value is set to zero.
                shortParams[SQL_MAX_COLUMNS_IN_INDEX] = 0;
#endif // SQL_MAX_COLUMNS_IN_INDEX

#ifdef SQL_MAX_COLUMNS_IN_ORDER_BY
                // Value that specifies the maximum number of columns allowed in an ORDER BY clause.
                // If there is no specified limit or the limit is unknown, this value is set to zero.
                // An FIPS Entry level-conformant driver will return at least 6. An FIPS Intermediate level-conformant
                // driver will return at least 15.
                shortParams[SQL_MAX_COLUMNS_IN_ORDER_BY] = 0;
#endif // SQL_MAX_COLUMNS_IN_ORDER_BY

#ifdef SQL_MAX_COLUMNS_IN_SELECT
                // Value that specifies the maximum number of columns allowed in a select list. If there is no specified
                // limit or the limit is unknown, this value is set to zero.
                // An FIPS Entry level-conformant driver will return at least 100. An FIPS Intermediate level-conformant
                // driver will return at least 250.
                shortParams[SQL_MAX_COLUMNS_IN_SELECT] = 0;
#endif // SQL_MAX_COLUMNS_IN_SELECT

#ifdef SQL_MAX_COLUMNS_IN_TABLE
                // Value that specifies the maximum number of columns allowed in a table. If there is no specified limit
                // or the limit is unknown, this value is set to zero.
                // An FIPS Entry level-conformant driver will return at least 100. An FIPS Intermediate level-conformant
                // driver will return at least 250.
                shortParams[SQL_MAX_COLUMNS_IN_TABLE] = 0;
#endif // SQL_MAX_COLUMNS_IN_TABLE

#ifdef SQL_MAX_CURSOR_NAME_LEN
                // Value that specifies the maximum length of a cursor name in the data source. If there is no maximum
                // length or the length is unknown, this value is set to zero.
                // An FIPS Entry level-conformant driver will return at least 18. An FIPS Intermediate level-conformant
                // driver will return at least 128.
                shortParams[SQL_MAX_CURSOR_NAME_LEN] = 0;
#endif // SQL_MAX_CURSOR_NAME_LEN

#ifdef SQL_MAX_DRIVER_CONNECTIONS
                // Value that specifies the maximum number of active connections that the driver can support for an
                // environment. This value can reflect a limitation imposed by either the driver or the data source.
                // If there is no specified limit or the limit is unknown, this value is set to zero.
                // This InfoType has been renamed for ODBC 3.0 from the ODBC 2.0 InfoType SQL_ACTIVE_CONNECTIONS.
                shortParams[SQL_MAX_DRIVER_CONNECTIONS] = 0;
#endif // SQL_MAX_DRIVER_CONNECTIONS

#ifdef SQL_MAX_IDENTIFIER_LEN
                // Value that indicates the maximum size in characters that the data source supports for user-defined
                // names.
                // An FIPS Entry level-conformant driver will return at least 18. An FIPS Intermediate level-conformant
                // driver will return at least 128.
                shortParams[SQL_MAX_IDENTIFIER_LEN] = 0;
#endif // SQL_MAX_IDENTIFIER_LEN

#ifdef SQL_MAX_PROCEDURE_NAME_LEN
                // Value that specifies the maximum length of a procedure name in the data source. If there is no
                // maximum length or the length is unknown, this value is set to zero.
                shortParams[SQL_MAX_PROCEDURE_NAME_LEN] = 0;
#endif // SQL_MAX_PROCEDURE_NAME_LEN

#ifdef SQL_MAX_SCHEMA_NAME_LEN
                // Value that specifies the maximum length of a schema name in the data source. If there is no maximum
                // length or the length is unknown, this value is set to zero.
                // An FIPS Entry level-conformant driver will return at least 18. An FIPS Intermediate level-conformant
                // driver will return at least 128.
                // This InfoType has been renamed for ODBC 3.0 from the ODBC 2.0 InfoType SQL_MAX_OWNER_NAME_LEN.
                shortParams[SQL_MAX_SCHEMA_NAME_LEN] = 0;
#endif // SQL_MAX_SCHEMA_NAME_LEN

#ifdef SQL_MAX_TABLE_NAME_LEN
                // Value that specifies the maximum length of a table name in the data source. If there is no maximum
                // length or the length is unknown, this value is set to zero.
                // An FIPS Entry level-conformant driver will return at least 18. An FIPS Intermediate level-conformant
                // driver will return at least 128.
                shortParams[SQL_MAX_TABLE_NAME_LEN] = 0;
#endif // SQL_MAX_TABLE_NAME_LEN

#ifdef SQL_MAX_TABLES_IN_SELECT
                // Value that specifies the maximum number of tables allowed in the FROM clause of a SELECT statement.
                // If there is no specified limit or the limit is unknown, this value is set to zero.
                // An FIPS Entry level-conformant driver will return at least 15. An FIPS Intermediate level-conformant
                // driver will return at least 50.
                shortParams[SQL_MAX_TABLES_IN_SELECT] = 0;
#endif // SQL_MAX_TABLES_IN_SELECT

#ifdef SQL_MAX_USER_NAME_LEN
                // Value that specifies the maximum length of a user name in the data source. If there is no maximum
                // length or the length is unknown, this value is set to zero.
                shortParams[SQL_MAX_USER_NAME_LEN] = 0;
#endif // SQL_MAX_USER_NAME_LEN

#ifdef SQL_NON_NULLABLE_COLUMNS
                // Value that specifies whether the data source supports NOT NULL in columns:
                // SQL_NNC_NULL = All columns must be nullable.
                // SQL_NNC_NON_NULL = Columns cannot be nullable. (The data source supports the NOT NULL column
                //     constraint in CREATE TABLE statements.)
                // An SQL-92 Entry level-conformant driver will return SQL_NNC_NON_NULL.
                shortParams[SQL_NON_NULLABLE_COLUMNS] = SQL_NNC_NULL;
#endif // SQL_NON_NULLABLE_COLUMNS

#ifdef SQL_NULL_COLLATION
                // Value that specifies where NULLs are sorted in a result set:
                // SQL_NC_END = NULLs are sorted at the end of the result set, regardless of the ASC or DESC keywords.
                // SQL_NC_HIGH = NULLs are sorted at the high end of the result set, depending on the ASC or DESC
                //     keywords.
                // SQL_NC_LOW = NULLs are sorted at the low end of the result set, depending on the ASC or DESC
                //     keywords.
                // SQL_NC_START = NULLs are sorted at the start of the result set, regardless of the ASC or DESC
                //     keywords.
                shortParams[SQL_NULL_COLLATION] = SQL_NC_END;
#endif // SQL_NULL_COLLATION
            }

            ConnectionInfo::~ConnectionInfo()
            {
                // No-op.
            }

            SqlResult::Type ConnectionInfo::GetInfo(InfoType type, void* buf,
                short buflen, short* reslen) const
            {
                if (!buf)
                    return SqlResult::AI_ERROR;

                StringInfoMap::const_iterator itStr = strParams.find(type);

                if (itStr != strParams.end())
                {
                    if (!buflen)
                        return SqlResult::AI_ERROR;

                    unsigned short strlen = static_cast<short>(
                        utility::CopyStringToBuffer(itStr->second,
                            reinterpret_cast<char*>(buf), buflen));

                    if (reslen)
                        *reslen = strlen;

                    return SqlResult::AI_SUCCESS;
                }

                UintInfoMap::const_iterator itInt = intParams.find(type);

                if (itInt != intParams.end())
                {
                    unsigned int *res = reinterpret_cast<unsigned int*>(buf);

                    *res = itInt->second;

                    return SqlResult::AI_SUCCESS;
                }

                UshortInfoMap::const_iterator itShort = shortParams.find(type);

                if (itShort != shortParams.end())
                {
                    unsigned short *res = reinterpret_cast<unsigned short*>(buf);

                    *res = itShort->second;

                    return SqlResult::AI_SUCCESS;
                }

                return SqlResult::AI_ERROR;
            }
        }
    }
}

