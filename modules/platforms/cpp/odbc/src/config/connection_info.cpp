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
                    DBG_STR_CASE(SQL_DRIVER_NAME);
                    DBG_STR_CASE(SQL_DBMS_NAME);
                    DBG_STR_CASE(SQL_DRIVER_ODBC_VER);
                    DBG_STR_CASE(SQL_DBMS_VER);
                    DBG_STR_CASE(SQL_DRIVER_VER);
                    DBG_STR_CASE(SQL_COLUMN_ALIAS);
                    DBG_STR_CASE(SQL_IDENTIFIER_QUOTE_CHAR);
                    DBG_STR_CASE(SQL_CATALOG_NAME_SEPARATOR);
                    DBG_STR_CASE(SQL_SPECIAL_CHARACTERS);
                    DBG_STR_CASE(SQL_CATALOG_TERM);
                    DBG_STR_CASE(SQL_TABLE_TERM);
                    DBG_STR_CASE(SQL_SCHEMA_TERM);
                    DBG_STR_CASE(SQL_NEED_LONG_DATA_LEN);
//                    DBG_STR_CASE(SQL_ASYNC_DBC_FUNCTIONS);
                    DBG_STR_CASE(SQL_ASYNC_NOTIFICATION);
                    DBG_STR_CASE(SQL_GETDATA_EXTENSIONS);
                    DBG_STR_CASE(SQL_ODBC_INTERFACE_CONFORMANCE);
                    DBG_STR_CASE(SQL_SQL_CONFORMANCE);
                    DBG_STR_CASE(SQL_CATALOG_USAGE);
                    DBG_STR_CASE(SQL_SCHEMA_USAGE);
                    DBG_STR_CASE(SQL_MAX_IDENTIFIER_LEN);
                    DBG_STR_CASE(SQL_AGGREGATE_FUNCTIONS);
                    DBG_STR_CASE(SQL_NUMERIC_FUNCTIONS);
                    DBG_STR_CASE(SQL_STRING_FUNCTIONS);
                    DBG_STR_CASE(SQL_TIMEDATE_FUNCTIONS);
                    DBG_STR_CASE(SQL_TIMEDATE_ADD_INTERVALS);
                    DBG_STR_CASE(SQL_TIMEDATE_DIFF_INTERVALS);
                    DBG_STR_CASE(SQL_DATETIME_LITERALS);
                    DBG_STR_CASE(SQL_SYSTEM_FUNCTIONS);
                    DBG_STR_CASE(SQL_CONVERT_FUNCTIONS);
                    DBG_STR_CASE(SQL_OJ_CAPABILITIES);
                    DBG_STR_CASE(SQL_POS_OPERATIONS);
                    DBG_STR_CASE(SQL_MAX_CONCURRENT_ACTIVITIES);
                    DBG_STR_CASE(SQL_CURSOR_COMMIT_BEHAVIOR);
                    DBG_STR_CASE(SQL_CURSOR_ROLLBACK_BEHAVIOR);
                    DBG_STR_CASE(SQL_TXN_CAPABLE);
                    DBG_STR_CASE(SQL_QUOTED_IDENTIFIER_CASE);
                    DBG_STR_CASE(SQL_SQL92_NUMERIC_VALUE_FUNCTIONS);
                    DBG_STR_CASE(SQL_SQL92_STRING_FUNCTIONS);
                    DBG_STR_CASE(SQL_SQL92_DATETIME_FUNCTIONS);
                    DBG_STR_CASE(SQL_SQL92_PREDICATES);
                    DBG_STR_CASE(SQL_SQL92_RELATIONAL_JOIN_OPERATORS);
                    DBG_STR_CASE(SQL_SQL92_VALUE_EXPRESSIONS);
                    DBG_STR_CASE(SQL_STATIC_CURSOR_ATTRIBUTES1);
                    DBG_STR_CASE(SQL_STATIC_CURSOR_ATTRIBUTES2);
                    DBG_STR_CASE(SQL_CONVERT_BIGINT);
                    DBG_STR_CASE(SQL_CONVERT_BINARY);
                    DBG_STR_CASE(SQL_CONVERT_BIT);
                    DBG_STR_CASE(SQL_CONVERT_CHAR);
                    DBG_STR_CASE(SQL_CONVERT_DATE);
                    DBG_STR_CASE(SQL_CONVERT_DECIMAL);
                    DBG_STR_CASE(SQL_CONVERT_DOUBLE);
                    DBG_STR_CASE(SQL_CONVERT_FLOAT);
                    DBG_STR_CASE(SQL_CONVERT_INTEGER);
                    DBG_STR_CASE(SQL_CONVERT_LONGVARCHAR);
                    DBG_STR_CASE(SQL_CONVERT_NUMERIC);
                    DBG_STR_CASE(SQL_CONVERT_REAL);
                    DBG_STR_CASE(SQL_CONVERT_SMALLINT);
                    DBG_STR_CASE(SQL_CONVERT_TIME);
                    DBG_STR_CASE(SQL_CONVERT_TIMESTAMP);
                    DBG_STR_CASE(SQL_CONVERT_TINYINT);
                    DBG_STR_CASE(SQL_CONVERT_VARBINARY);
                    DBG_STR_CASE(SQL_CONVERT_VARCHAR);
                    DBG_STR_CASE(SQL_CONVERT_LONGVARBINARY);
                    DBG_STR_CASE(SQL_CONVERT_WCHAR);
                    DBG_STR_CASE(SQL_CONVERT_INTERVAL_DAY_TIME);
                    DBG_STR_CASE(SQL_CONVERT_INTERVAL_YEAR_MONTH);
                    DBG_STR_CASE(SQL_CONVERT_WLONGVARCHAR);
                    DBG_STR_CASE(SQL_CONVERT_WVARCHAR);
                    DBG_STR_CASE(SQL_CONVERT_GUID);
                default:
                    break;
                }
                return "<< UNKNOWN TYPE >>";
            }

#undef DBG_STR_CASE

            ConnectionInfo::ConnectionInfo() : strParams(), intParams(),
                shortParams()
            {
                //======================= String Params =======================
                // Driver name.
                strParams[SQL_DRIVER_NAME] = "Apache Ignite";
                strParams[SQL_DBMS_NAME]   = "Apache Ignite";

                // ODBC version.
                strParams[SQL_DRIVER_ODBC_VER] = "03.00";
                strParams[SQL_DBMS_VER]        = "03.00";

#ifdef SQL_DRIVER_VER
                // Driver version. At a minimum, the version is of the form
                // ##.##.####, where the first two digits are the major version,
                // the next two digits are the minor version, and the last four
                // digits are the release version.
                strParams[SQL_DRIVER_VER] = "01.05.0000";
#endif // SQL_DRIVER_VER

#ifdef SQL_COLUMN_ALIAS
                // A character string: "Y" if the data source supports column
                // aliases; otherwise, "N".
                strParams[SQL_COLUMN_ALIAS] = "Y";
#endif // SQL_COLUMN_ALIAS

#ifdef SQL_IDENTIFIER_QUOTE_CHAR
                // The character string that is used as the starting and ending
                // delimiter of a quoted (delimited) identifier in SQL statements.
                // Identifiers passed as arguments to ODBC functions do not have to
                // be quoted. If the data source does not support quoted
                // identifiers, a blank is returned.
                strParams[SQL_IDENTIFIER_QUOTE_CHAR] = "";
#endif // SQL_IDENTIFIER_QUOTE_CHAR

#ifdef SQL_CATALOG_NAME_SEPARATOR
                // A character string: the character or characters that the data
                // source defines as the separator between a catalog name and the
                // qualified name element that follows or precedes it.
                strParams[SQL_CATALOG_NAME_SEPARATOR] = ".";
#endif // SQL_CATALOG_NAME_SEPARATOR

#ifdef SQL_SPECIAL_CHARACTERS
                // A character string that contains all special characters (that
                // is, all characters except a through z, A through Z, 0 through 9,
                // and underscore) that can be used in an identifier name, such as
                // a table name, column name, or index name, on the data source.
                strParams[SQL_SPECIAL_CHARACTERS] = "";
#endif // SQL_SPECIAL_CHARACTERS

#ifdef SQL_CATALOG_TERM
                // A character string with the data source vendor's name for
                // a catalog; for example, "database" or "directory". This string
                // can be in upper, lower, or mixed case.
                strParams[SQL_CATALOG_TERM] = "catalog";
#endif // SQL_CATALOG_TERM

#ifdef SQL_TABLE_TERM
                // A character string with the data source vendor's name for
                // a table; for example, "table" or "file".
                strParams[SQL_TABLE_TERM] = "table";
#endif // SQL_TABLE_TERM

#ifdef SQL_SCHEMA_TERM
                // A character string with the data source vendor's name for
                // a schema; for example, "owner", "Authorization ID", or "Schema".
                strParams[SQL_SCHEMA_TERM] = "schema";
#endif // SQL_SCHEMA_TERM

#ifdef SQL_NEED_LONG_DATA_LEN
                // A character string: "Y" if the data source needs the length
                // of a long data value (the data type is SQL_LONGVARCHAR,
                // SQL_LONGVARBINARY) before that value is sent to the data
                // source, "N" if it does not.
                strParams[SQL_NEED_LONG_DATA_LEN ] = "Y";
#endif // SQL_NEED_LONG_DATA_LEN

#ifdef SQL_ASYNC_DBC_FUNCTIONS
                //====================== Integer Params =======================
                // Indicates if the driver can execute functions asynchronously
                // on the connection handle.
                // SQL_ASYNC_DBC_CAPABLE = The driver can execute connection
                // functions asynchronously.
                // SQL_ASYNC_DBC_NOT_CAPABLE = The driver can not execute
                // connection functions asynchronously.
                intParams[SQL_ASYNC_DBC_FUNCTIONS] = SQL_ASYNC_DBC_NOT_CAPABLE;
#endif // SQL_ASYNC_DBC_FUNCTIONS

#ifdef SQL_ASYNC_NOTIFICATION
                // Indicates if the driver supports asynchronous notification.
                // SQL_ASYNC_NOTIFICATION_CAPABLE  = Asynchronous execution
                // notification is supported by the driver.
                // SQL_ASYNC_NOTIFICATION_NOT_CAPABLE Asynchronous execution
                // notification is not supported by the driver.
                intParams[SQL_ASYNC_NOTIFICATION] = SQL_ASYNC_NOTIFICATION_NOT_CAPABLE;
#endif // SQL_ASYNC_NOTIFICATION

#ifdef SQL_GETDATA_EXTENSIONS
                // Bitmask enumerating extensions to SQLGetData.
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
                intParams[SQL_CATALOG_USAGE] = 0;
#endif // SQL_CATALOG_USAGE

#ifdef SQL_SCHEMA_USAGE
                // Bitmask enumerating the statements in which schemas can be used.
                intParams[SQL_SCHEMA_USAGE] = SQL_SU_DML_STATEMENTS |
                    SQL_SU_TABLE_DEFINITION | SQL_SU_PRIVILEGE_DEFINITION;
#endif // SQL_SCHEMA_USAGE

#ifdef SQL_MAX_IDENTIFIER_LEN
                // Indicates the maximum size in characters that the data source
                // supports for user-defined names.
                intParams[SQL_MAX_IDENTIFIER_LEN] = 128;
#endif // SQL_MAX_IDENTIFIER_LEN

#ifdef SQL_AGGREGATE_FUNCTIONS
                // Bitmask enumerating support for aggregation functions.
                intParams[SQL_AGGREGATE_FUNCTIONS] = SQL_AF_AVG | SQL_AF_COUNT |
                    SQL_AF_DISTINCT | SQL_AF_MAX | SQL_AF_MIN | SQL_AF_SUM;
#endif // SQL_AGGREGATE_FUNCTIONS

#ifdef SQL_NUMERIC_FUNCTIONS
                // Bitmask enumerating the scalar numeric functions supported by
                // the driver and associated data source.
                intParams[SQL_NUMERIC_FUNCTIONS] = SQL_FN_NUM_ABS | SQL_FN_NUM_ACOS | SQL_FN_NUM_ASIN |
                    SQL_FN_NUM_ATAN | SQL_FN_NUM_ATAN2 | SQL_FN_NUM_CEILING | SQL_FN_NUM_COS | SQL_FN_NUM_COT |
                    SQL_FN_NUM_EXP | SQL_FN_NUM_FLOOR | SQL_FN_NUM_LOG | SQL_FN_NUM_MOD | SQL_FN_NUM_SIGN |
                    SQL_FN_NUM_SIN | SQL_FN_NUM_SQRT | SQL_FN_NUM_TAN | SQL_FN_NUM_PI | SQL_FN_NUM_RAND |
                    SQL_FN_NUM_DEGREES | SQL_FN_NUM_LOG10 | SQL_FN_NUM_POWER | SQL_FN_NUM_RADIANS | SQL_FN_NUM_ROUND |
                    SQL_FN_NUM_TRUNCATE;
#endif // SQL_NUMERIC_FUNCTIONS

#ifdef SQL_STRING_FUNCTIONS
                // Bitmask enumerating the scalar string functions supported by the
                // driver and associated data source.
                intParams[SQL_STRING_FUNCTIONS] = SQL_FN_STR_ASCII | SQL_FN_STR_BIT_LENGTH | SQL_FN_STR_CHAR |
                    SQL_FN_STR_CONCAT | SQL_FN_STR_DIFFERENCE | SQL_FN_STR_INSERT | SQL_FN_STR_LEFT |
                    SQL_FN_STR_LENGTH | SQL_FN_STR_LOCATE | SQL_FN_STR_LTRIM | SQL_FN_STR_OCTET_LENGTH |
                    SQL_FN_STR_POSITION | SQL_FN_STR_REPEAT | SQL_FN_STR_REPLACE | SQL_FN_STR_RIGHT | SQL_FN_STR_RTRIM |
                    SQL_FN_STR_SOUNDEX | SQL_FN_STR_SPACE | SQL_FN_STR_SUBSTRING | SQL_FN_STR_LCASE | SQL_FN_STR_UCASE |
                    SQL_FN_STR_LOCATE_2 | SQL_FN_STR_CHAR_LENGTH | SQL_FN_STR_CHARACTER_LENGTH;
#endif // SQL_STRING_FUNCTIONS

#ifdef SQL_TIMEDATE_FUNCTIONS
                // Bitmask enumerating the scalar date and time functions supported
                // by the driver and associated data source.
                intParams[SQL_TIMEDATE_FUNCTIONS] = SQL_FN_TD_CURRENT_DATE | SQL_FN_TD_CURRENT_TIME |
                    SQL_FN_TD_CURRENT_TIMESTAMP | SQL_FN_TD_CURDATE | SQL_FN_TD_CURTIME | SQL_FN_TD_DAYNAME |
                    SQL_FN_TD_DAYOFMONTH | SQL_FN_TD_DAYOFWEEK | SQL_FN_TD_DAYOFYEAR | SQL_FN_TD_EXTRACT |
                    SQL_FN_TD_HOUR | SQL_FN_TD_MINUTE | SQL_FN_TD_MONTH | SQL_FN_TD_MONTHNAME | SQL_FN_TD_NOW |
                    SQL_FN_TD_QUARTER | SQL_FN_TD_SECOND | SQL_FN_TD_WEEK | SQL_FN_TD_YEAR;
#endif // SQL_TIMEDATE_FUNCTIONS

#ifdef SQL_TIMEDATE_ADD_INTERVALS
                // Bitmask enumerating timestamp intervals supported by the driver
                // and associated data source for the TIMESTAMPADD scalar function.
                intParams[SQL_TIMEDATE_ADD_INTERVALS] = 0;
#endif // SQL_TIMEDATE_ADD_INTERVALS

#ifdef SQL_TIMEDATE_DIFF_INTERVALS
                // Bitmask enumerating timestamp intervals supported by the driver
                // and associated data source for the TIMESTAMPDIFF scalar function.
                intParams[SQL_TIMEDATE_DIFF_INTERVALS] = 0;
#endif // SQL_TIMEDATE_DIFF_INTERVALS

#ifdef SQL_DATETIME_LITERALS
                // Bitmask enumerating the SQL-92 datetime literals supported by
                // the data source.
                intParams[SQL_DATETIME_LITERALS] =  SQL_DL_SQL92_DATE | SQL_DL_SQL92_TIME | SQL_DL_SQL92_TIMESTAMP;
#endif // SQL_DATETIME_LITERALS

#ifdef SQL_SYSTEM_FUNCTIONS
                // Bitmask enumerating the scalar system functions supported by the
                // driver and associated data source.
                intParams[SQL_SYSTEM_FUNCTIONS] = SQL_FN_SYS_USERNAME | SQL_FN_SYS_DBNAME | SQL_FN_SYS_IFNULL;
#endif // SQL_SYSTEM_FUNCTIONS

#ifdef SQL_CONVERT_FUNCTIONS
                // Bitmask enumerating the scalar conversion functions supported
                // by the driver and associated data source.
                intParams[SQL_CONVERT_FUNCTIONS] = SQL_FN_CVT_CONVERT | SQL_FN_CVT_CAST;
#endif // SQL_CONVERT_FUNCTIONS

#ifdef SQL_OJ_CAPABILITIES
                // Bitmask enumerating the types of outer joins supported by the
                // driver and data source.
                intParams[SQL_OJ_CAPABILITIES] = SQL_OJ_LEFT | SQL_OJ_NOT_ORDERED | SQL_OJ_ALL_COMPARISON_OPS;
#endif // SQL_OJ_CAPABILITIES

#ifdef SQL_POS_OPERATIONS
                // Bitmask enumerating the support operations in SQLSetPos.
                intParams[SQL_POS_OPERATIONS] = 0;
#endif // SQL_POS_OPERATIONS

#ifdef SQL_SQL92_NUMERIC_VALUE_FUNCTIONS
                // Bitmask enumerating the numeric value scalar functions.
                intParams[SQL_SQL92_NUMERIC_VALUE_FUNCTIONS] = 0;
#endif // SQL_SQL92_NUMERIC_VALUE_FUNCTIONS

#ifdef SQL_SQL92_STRING_FUNCTIONS
                // Bitmask enumerating the string scalar functions.
                intParams[SQL_SQL92_STRING_FUNCTIONS] = SQL_SSF_LOWER | SQL_SSF_UPPER | SQL_SSF_SUBSTRING |
                    SQL_SSF_TRIM_BOTH | SQL_SSF_TRIM_LEADING | SQL_SSF_TRIM_TRAILING;
#endif // SQL_SQL92_STRING_FUNCTIONS

#ifdef SQL_SQL92_DATETIME_FUNCTIONS
                // Bitmask enumerating the datetime scalar functions.
                intParams[SQL_SQL92_DATETIME_FUNCTIONS] = SQL_SDF_CURRENT_DATE |
                    SQL_SDF_CURRENT_TIMESTAMP;
#endif // SQL_SQL92_DATETIME_FUNCTIONS

#ifdef SQL_SQL92_VALUE_EXPRESSIONS
                // Bitmask enumerating the value expressions supported,
                // as defined in SQL-92.
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
                // Bitmask enumerating the relational join operators supported
                // in a SELECT statement, as defined in SQL-92.
                intParams[SQL_SQL92_RELATIONAL_JOIN_OPERATORS] = SQL_SRJO_CORRESPONDING_CLAUSE | SQL_SRJO_CROSS_JOIN |
                    SQL_SRJO_EXCEPT_JOIN | SQL_SRJO_INNER_JOIN | SQL_SRJO_LEFT_OUTER_JOIN| SQL_SRJO_RIGHT_OUTER_JOIN |
                    SQL_SRJO_NATURAL_JOIN | SQL_SRJO_INTERSECT_JOIN | SQL_SRJO_UNION_JOIN;
#endif // SQL_SQL92_RELATIONAL_JOIN_OPERATORS

#ifdef SQL_STATIC_CURSOR_ATTRIBUTES1
                // Bitmask that describes the attributes of a static cursor that
                // are supported by the driver. This bitmask contains the first
                // subset of attributes; for the second subset, see
                // SQL_STATIC_CURSOR_ATTRIBUTES2.
                intParams[SQL_STATIC_CURSOR_ATTRIBUTES1] = SQL_CA1_NEXT;
#endif //SQL_STATIC_CURSOR_ATTRIBUTES1

#ifdef SQL_STATIC_CURSOR_ATTRIBUTES2
                // Bitmask that describes the attributes of a static cursor that
                // are supported by the driver. This bitmask contains the second
                // subset of attributes; for the first subset, see
                // SQL_STATIC_CURSOR_ATTRIBUTES1.
                intParams[SQL_STATIC_CURSOR_ATTRIBUTES2] = 0;
#endif //SQL_STATIC_CURSOR_ATTRIBUTES2

#ifdef SQL_CONVERT_BIGINT
                // Bitmask indicates the conversions supported by the CONVERT scalar function for target type BIGINT
                intParams[SQL_CONVERT_BIGINT] = SQL_CVT_CHAR | SQL_CVT_VARCHAR | SQL_CVT_LONGVARCHAR | SQL_CVT_WCHAR |
                    SQL_CVT_WLONGVARCHAR | SQL_CVT_WVARCHAR | SQL_CVT_NUMERIC | SQL_CVT_BIT |
                    SQL_CVT_TINYINT | SQL_CVT_SMALLINT | SQL_CVT_INTEGER | SQL_CVT_BIGINT |
                    SQL_CVT_TIMESTAMP;
#endif //SQL_CONVERT_BIGINT

#ifdef SQL_CONVERT_BINARY
                // Bitmask indicates the conversions supported by the CONVERT scalar function for target type BINARY
                intParams[SQL_CONVERT_BINARY] = SQL_CVT_CHAR | SQL_CVT_VARCHAR | SQL_CVT_LONGVARCHAR |
                    SQL_CVT_WCHAR | SQL_CVT_WLONGVARCHAR | SQL_CVT_WVARCHAR | SQL_CVT_NUMERIC | SQL_CVT_DECIMAL |
                    SQL_CVT_BIT | SQL_CVT_TINYINT | SQL_CVT_SMALLINT | SQL_CVT_INTEGER | SQL_CVT_BIGINT | SQL_CVT_REAL |
                    SQL_CVT_FLOAT | SQL_CVT_DOUBLE | SQL_CVT_BINARY | SQL_CVT_VARBINARY | SQL_CVT_LONGVARBINARY |
                    SQL_CVT_DATE | SQL_CVT_TIME | SQL_CVT_TIMESTAMP | SQL_CVT_GUID;
#endif //SQL_CONVERT_BINARY

#ifdef SQL_CONVERT_BIT
                // Bitmask indicates the conversions supported by the CONVERT scalar function for target type BIT
                intParams[SQL_CONVERT_BIT] = SQL_CVT_CHAR | SQL_CVT_VARCHAR | SQL_CVT_LONGVARCHAR |
                    SQL_CVT_WCHAR | SQL_CVT_WLONGVARCHAR | SQL_CVT_WVARCHAR | SQL_CVT_NUMERIC |
                    SQL_CVT_BIT | SQL_CVT_TINYINT | SQL_CVT_SMALLINT | SQL_CVT_INTEGER | SQL_CVT_BIGINT;
#endif //SQL_CONVERT_BIT

#ifdef SQL_CONVERT_CHAR
                // Bitmask indicates the conversions supported by the CONVERT scalar function for target type CHAR
                intParams[SQL_CONVERT_CHAR] = SQL_CVT_CHAR | SQL_CVT_VARCHAR | SQL_CVT_LONGVARCHAR |
                    SQL_CVT_WCHAR | SQL_CVT_WLONGVARCHAR | SQL_CVT_WVARCHAR |
                    SQL_CVT_NUMERIC | SQL_CVT_DECIMAL | SQL_CVT_BIT | SQL_CVT_TINYINT | SQL_CVT_SMALLINT | SQL_CVT_INTEGER |
                    SQL_CVT_BIGINT | SQL_CVT_REAL | SQL_CVT_FLOAT | SQL_CVT_DOUBLE | SQL_CVT_BINARY | SQL_CVT_VARBINARY |
                    SQL_CVT_LONGVARBINARY | SQL_CVT_DATE | SQL_CVT_TIME | SQL_CVT_TIMESTAMP | SQL_CVT_GUID;
#endif //SQL_CONVERT_CHAR

#ifdef SQL_CONVERT_VARCHAR
                // Bitmask indicates the conversions supported by the CONVERT scalar function for target type VARCHAR
                intParams[SQL_CONVERT_VARCHAR] = SQL_CVT_CHAR | SQL_CVT_VARCHAR | SQL_CVT_LONGVARCHAR |
                    SQL_CVT_WCHAR | SQL_CVT_WLONGVARCHAR | SQL_CVT_WVARCHAR |
                    SQL_CVT_NUMERIC | SQL_CVT_DECIMAL | SQL_CVT_BIT | SQL_CVT_TINYINT | SQL_CVT_SMALLINT | SQL_CVT_INTEGER |
                    SQL_CVT_BIGINT | SQL_CVT_REAL | SQL_CVT_FLOAT | SQL_CVT_DOUBLE | SQL_CVT_BINARY | SQL_CVT_VARBINARY |
                    SQL_CVT_LONGVARBINARY | SQL_CVT_DATE | SQL_CVT_TIME | SQL_CVT_TIMESTAMP | SQL_CVT_GUID;
#endif //SQL_CONVERT_VARCHAR

#ifdef SQL_CONVERT_LONGVARCHAR
                // Bitmask indicates the conversions supported by the CONVERT scalar function for target type LONGVARCHAR
                intParams[SQL_CONVERT_LONGVARCHAR] = SQL_CVT_CHAR | SQL_CVT_VARCHAR | SQL_CVT_LONGVARCHAR |
                    SQL_CVT_WCHAR | SQL_CVT_WLONGVARCHAR | SQL_CVT_WVARCHAR |
                    SQL_CVT_NUMERIC | SQL_CVT_DECIMAL | SQL_CVT_BIT | SQL_CVT_TINYINT | SQL_CVT_SMALLINT | SQL_CVT_INTEGER |
                    SQL_CVT_BIGINT | SQL_CVT_REAL | SQL_CVT_FLOAT | SQL_CVT_DOUBLE | SQL_CVT_BINARY | SQL_CVT_VARBINARY |
                    SQL_CVT_LONGVARBINARY | SQL_CVT_DATE | SQL_CVT_TIME | SQL_CVT_TIMESTAMP | SQL_CVT_GUID;
#endif //SQL_CONVERT_LONGVARCHAR

#ifdef SQL_CONVERT_WCHAR
                // Bitmask indicates the conversions supported by the CONVERT scalar function for target type WCHAR
                intParams[SQL_CONVERT_WCHAR] = SQL_CVT_CHAR | SQL_CVT_VARCHAR | SQL_CVT_LONGVARCHAR |
                    SQL_CVT_WCHAR | SQL_CVT_WLONGVARCHAR | SQL_CVT_WVARCHAR |
                    SQL_CVT_NUMERIC | SQL_CVT_DECIMAL | SQL_CVT_BIT | SQL_CVT_TINYINT | SQL_CVT_SMALLINT | SQL_CVT_INTEGER |
                    SQL_CVT_BIGINT | SQL_CVT_REAL | SQL_CVT_FLOAT | SQL_CVT_DOUBLE | SQL_CVT_BINARY | SQL_CVT_VARBINARY |
                    SQL_CVT_LONGVARBINARY | SQL_CVT_DATE | SQL_CVT_TIME | SQL_CVT_TIMESTAMP | SQL_CVT_GUID;
#endif //SQL_CONVERT_WCHAR

#ifdef SQL_CONVERT_WVARCHAR
                // Bitmask indicates the conversions supported by the CONVERT scalar function for target type WVARCHAR
                intParams[SQL_CONVERT_WVARCHAR] = SQL_CVT_CHAR | SQL_CVT_VARCHAR | SQL_CVT_LONGVARCHAR |
                    SQL_CVT_WCHAR | SQL_CVT_WLONGVARCHAR | SQL_CVT_WVARCHAR |
                    SQL_CVT_NUMERIC | SQL_CVT_DECIMAL | SQL_CVT_BIT | SQL_CVT_TINYINT | SQL_CVT_SMALLINT | SQL_CVT_INTEGER |
                    SQL_CVT_BIGINT | SQL_CVT_REAL | SQL_CVT_FLOAT | SQL_CVT_DOUBLE | SQL_CVT_BINARY | SQL_CVT_VARBINARY |
                    SQL_CVT_LONGVARBINARY | SQL_CVT_DATE | SQL_CVT_TIME | SQL_CVT_TIMESTAMP | SQL_CVT_GUID;
#endif //SQL_CONVERT_WVARCHAR

#ifdef SQL_CONVERT_WLONGVARCHAR
                // Bitmask indicates the conversions supported by the CONVERT scalar function for target type WLONGVARCHAR
                intParams[SQL_CONVERT_WLONGVARCHAR] = SQL_CVT_CHAR | SQL_CVT_VARCHAR | SQL_CVT_LONGVARCHAR |
                    SQL_CVT_WCHAR | SQL_CVT_WLONGVARCHAR | SQL_CVT_WVARCHAR |
                    SQL_CVT_NUMERIC | SQL_CVT_DECIMAL | SQL_CVT_BIT | SQL_CVT_TINYINT | SQL_CVT_SMALLINT | SQL_CVT_INTEGER |
                    SQL_CVT_BIGINT | SQL_CVT_REAL | SQL_CVT_FLOAT | SQL_CVT_DOUBLE | SQL_CVT_BINARY | SQL_CVT_VARBINARY |
                    SQL_CVT_LONGVARBINARY | SQL_CVT_DATE | SQL_CVT_TIME | SQL_CVT_TIMESTAMP | SQL_CVT_GUID;
#endif //SQL_CONVERT_WLONGVARCHAR

#ifdef SQL_CONVERT_DATE
                // Bitmask indicates the conversions supported by the CONVERT scalar function for target type DATE
                intParams[SQL_CONVERT_DATE] = SQL_CVT_CHAR | SQL_CVT_VARCHAR | SQL_CVT_LONGVARCHAR |
                    SQL_CVT_WCHAR | SQL_CVT_WLONGVARCHAR | SQL_CVT_WVARCHAR |
                    SQL_CVT_NUMERIC | SQL_CVT_INTEGER | SQL_CVT_BIGINT | SQL_CVT_BINARY | SQL_CVT_VARBINARY | SQL_CVT_LONGVARBINARY |
                    SQL_CVT_DATE | SQL_CVT_TIMESTAMP;
#endif //SQL_CONVERT_DATE

#ifdef SQL_CONVERT_DECIMAL
                // Bitmask indicates the conversions supported by the CONVERT scalar function for target type DECIMAL
                intParams[SQL_CONVERT_DECIMAL] = SQL_CVT_CHAR | SQL_CVT_VARCHAR | SQL_CVT_LONGVARCHAR |
                    SQL_CVT_WCHAR | SQL_CVT_WLONGVARCHAR | SQL_CVT_WVARCHAR |
                    SQL_CVT_NUMERIC | SQL_CVT_DECIMAL | SQL_CVT_BIT | SQL_CVT_TINYINT | SQL_CVT_SMALLINT | SQL_CVT_INTEGER | SQL_CVT_BIGINT |
                    SQL_CVT_REAL | SQL_CVT_FLOAT | SQL_CVT_DOUBLE | SQL_CVT_BINARY | SQL_CVT_VARBINARY | SQL_CVT_LONGVARBINARY | SQL_CVT_TIMESTAMP;
#endif //SQL_CONVERT_DECIMAL

#ifdef SQL_CONVERT_DOUBLE
                // Bitmask indicates the conversions supported by the CONVERT scalar function for target type DOUBLE
                intParams[SQL_CONVERT_DOUBLE] = SQL_CVT_CHAR | SQL_CVT_VARCHAR | SQL_CVT_LONGVARCHAR | SQL_CVT_WCHAR | SQL_CVT_WLONGVARCHAR | SQL_CVT_WVARCHAR |
                    SQL_CVT_NUMERIC | SQL_CVT_DECIMAL | SQL_CVT_BIT | SQL_CVT_TINYINT | SQL_CVT_SMALLINT | SQL_CVT_INTEGER |
                    SQL_CVT_BIGINT | SQL_CVT_REAL | SQL_CVT_FLOAT | SQL_CVT_DOUBLE | SQL_CVT_BINARY | SQL_CVT_VARBINARY |
                    SQL_CVT_LONGVARBINARY | SQL_CVT_TIMESTAMP;
#endif //SQL_CONVERT_DOUBLE

#ifdef SQL_CONVERT_FLOAT
                // Bitmask indicates the conversions supported by the CONVERT scalar function for target type FLOAT
                intParams[SQL_CONVERT_FLOAT] = SQL_CVT_CHAR | SQL_CVT_VARCHAR | SQL_CVT_LONGVARCHAR |
                    SQL_CVT_WCHAR | SQL_CVT_WLONGVARCHAR | SQL_CVT_WVARCHAR |
                    SQL_CVT_NUMERIC | SQL_CVT_DECIMAL | SQL_CVT_BIT | SQL_CVT_TINYINT | SQL_CVT_SMALLINT | SQL_CVT_INTEGER |
                    SQL_CVT_BIGINT | SQL_CVT_REAL | SQL_CVT_FLOAT | SQL_CVT_DOUBLE | SQL_CVT_BINARY | SQL_CVT_VARBINARY |
                    SQL_CVT_LONGVARBINARY | SQL_CVT_TIMESTAMP;
#endif //SQL_CONVERT_FLOAT

#ifdef SQL_CONVERT_REAL
                // Bitmask indicates the conversions supported by the CONVERT scalar function for target type REAL
                intParams[SQL_CONVERT_REAL] = SQL_CVT_CHAR | SQL_CVT_VARCHAR | SQL_CVT_LONGVARCHAR |
                    SQL_CVT_WCHAR | SQL_CVT_WLONGVARCHAR | SQL_CVT_WVARCHAR |
                    SQL_CVT_NUMERIC | SQL_CVT_DECIMAL | SQL_CVT_BIT | SQL_CVT_TINYINT | SQL_CVT_SMALLINT |
                    SQL_CVT_INTEGER | SQL_CVT_BIGINT | SQL_CVT_REAL | SQL_CVT_FLOAT | SQL_CVT_DOUBLE |
                    SQL_CVT_BINARY | SQL_CVT_VARBINARY | SQL_CVT_LONGVARBINARY | SQL_CVT_TIMESTAMP;
#endif //SQL_CONVERT_REAL

#ifdef SQL_CONVERT_INTEGER
                // Bitmask indicates the conversions supported by the CONVERT scalar function for target type INTEGER
                intParams[SQL_CONVERT_INTEGER] = SQL_CVT_CHAR | SQL_CVT_VARCHAR | SQL_CVT_LONGVARCHAR |
                    SQL_CVT_WCHAR | SQL_CVT_WLONGVARCHAR | SQL_CVT_WVARCHAR |
                    SQL_CVT_NUMERIC | SQL_CVT_DECIMAL | SQL_CVT_BIT | SQL_CVT_TINYINT | SQL_CVT_SMALLINT |
                    SQL_CVT_INTEGER | SQL_CVT_BIGINT | SQL_CVT_REAL | SQL_CVT_FLOAT | SQL_CVT_DOUBLE | SQL_CVT_BINARY |
                    SQL_CVT_VARBINARY | SQL_CVT_LONGVARBINARY | SQL_CVT_TIMESTAMP;
#endif //SQL_CONVERT_INTEGER

#ifdef SQL_CONVERT_NUMERIC
                // Bitmask indicates the conversions supported by the CONVERT scalar function for target type NUMERIC
                intParams[SQL_CONVERT_NUMERIC] = SQL_CVT_CHAR | SQL_CVT_VARCHAR | SQL_CVT_LONGVARCHAR |
                    SQL_CVT_WCHAR | SQL_CVT_WLONGVARCHAR | SQL_CVT_WVARCHAR |
                    SQL_CVT_NUMERIC | SQL_CVT_DECIMAL | SQL_CVT_BIT | SQL_CVT_TINYINT |
                    SQL_CVT_SMALLINT | SQL_CVT_INTEGER | SQL_CVT_BIGINT | SQL_CVT_REAL |
                    SQL_CVT_FLOAT | SQL_CVT_DOUBLE | SQL_CVT_BINARY | SQL_CVT_VARBINARY |
                    SQL_CVT_LONGVARBINARY | SQL_CVT_TIMESTAMP;
#endif //SQL_CONVERT_NUMERIC

#ifdef SQL_CONVERT_SMALLINT
                // Bitmask indicates the conversions supported by the CONVERT scalar function for target type SMALLINT
                intParams[SQL_CONVERT_SMALLINT] = SQL_CVT_CHAR | SQL_CVT_VARCHAR | SQL_CVT_LONGVARCHAR |
                    SQL_CVT_WCHAR | SQL_CVT_WLONGVARCHAR | SQL_CVT_WVARCHAR |
                    SQL_CVT_NUMERIC | SQL_CVT_DECIMAL | SQL_CVT_BIT | SQL_CVT_TINYINT |
                    SQL_CVT_SMALLINT | SQL_CVT_INTEGER | SQL_CVT_BIGINT | SQL_CVT_REAL |
                    SQL_CVT_FLOAT | SQL_CVT_DOUBLE | SQL_CVT_BINARY | SQL_CVT_VARBINARY |
                    SQL_CVT_LONGVARBINARY | SQL_CVT_TIMESTAMP;
#endif //SQL_CONVERT_SMALLINT

#ifdef SQL_CONVERT_TINYINT
                // Bitmask indicates the conversions supported by the CONVERT scalar function for target type TINYINT
                intParams[SQL_CONVERT_TINYINT] = SQL_CVT_CHAR | SQL_CVT_VARCHAR | SQL_CVT_LONGVARCHAR |
                    SQL_CVT_WCHAR | SQL_CVT_WLONGVARCHAR | SQL_CVT_WVARCHAR |
                    SQL_CVT_NUMERIC | SQL_CVT_DECIMAL | SQL_CVT_BIT | SQL_CVT_TINYINT | SQL_CVT_SMALLINT |
                    SQL_CVT_INTEGER | SQL_CVT_BIGINT | SQL_CVT_REAL | SQL_CVT_FLOAT | SQL_CVT_DOUBLE |
                    SQL_CVT_BINARY | SQL_CVT_VARBINARY | SQL_CVT_LONGVARBINARY | SQL_CVT_TIMESTAMP;
#endif //SQL_CONVERT_TINYINT

#ifdef SQL_CONVERT_TIME
                // Bitmask indicates the conversions supported by the CONVERT scalar function for target type TIME
                intParams[SQL_CONVERT_TIME] = SQL_CVT_CHAR | SQL_CVT_VARCHAR | SQL_CVT_LONGVARCHAR |
                    SQL_CVT_WCHAR | SQL_CVT_WLONGVARCHAR | SQL_CVT_WVARCHAR |
                    SQL_CVT_BINARY | SQL_CVT_VARBINARY | SQL_CVT_LONGVARBINARY |
                    SQL_CVT_TIME | SQL_CVT_TIMESTAMP;
#endif //SQL_CONVERT_TIME

#ifdef SQL_CONVERT_TIMESTAMP
                // Bitmask indicates the conversions supported by the CONVERT scalar function for target type TIMESTAMP
                intParams[SQL_CONVERT_TIMESTAMP] = SQL_CVT_CHAR | SQL_CVT_VARCHAR | SQL_CVT_LONGVARCHAR |
                    SQL_CVT_WCHAR | SQL_CVT_WLONGVARCHAR | SQL_CVT_WVARCHAR |
                    SQL_CVT_NUMERIC | SQL_CVT_DECIMAL | SQL_CVT_INTEGER | SQL_CVT_BIGINT | SQL_CVT_BINARY |
                    SQL_CVT_VARBINARY | SQL_CVT_LONGVARBINARY | SQL_CVT_DATE | SQL_CVT_TIME | SQL_CVT_TIMESTAMP;
#endif //SQL_CONVERT_TIMESTAMP

#ifdef SQL_CONVERT_VARBINARY
                // Bitmask indicates the conversions supported by the CONVERT scalar function for target type VARBINARY
                intParams[SQL_CONVERT_VARBINARY] = SQL_CVT_CHAR | SQL_CVT_VARCHAR | SQL_CVT_LONGVARCHAR |
                    SQL_CVT_WCHAR | SQL_CVT_WLONGVARCHAR | SQL_CVT_WVARCHAR |
                    SQL_CVT_NUMERIC | SQL_CVT_DECIMAL | SQL_CVT_BIT | SQL_CVT_TINYINT | SQL_CVT_SMALLINT |
                    SQL_CVT_INTEGER | SQL_CVT_BIGINT | SQL_CVT_REAL | SQL_CVT_FLOAT | SQL_CVT_DOUBLE |
                    SQL_CVT_BINARY | SQL_CVT_VARBINARY | SQL_CVT_LONGVARBINARY | SQL_CVT_DATE |
                    SQL_CVT_TIME | SQL_CVT_TIMESTAMP | SQL_CVT_GUID;
#endif //SQL_CONVERT_VARBINARY

#ifdef SQL_CONVERT_LONGVARBINARY
                // Bitmask indicates the conversions supported by the CONVERT scalar function for target type LONGVARBINARY
                intParams[SQL_CONVERT_LONGVARBINARY] = SQL_CVT_CHAR | SQL_CVT_VARCHAR | SQL_CVT_LONGVARCHAR |
                    SQL_CVT_WCHAR | SQL_CVT_WLONGVARCHAR | SQL_CVT_WVARCHAR |
                    SQL_CVT_NUMERIC | SQL_CVT_DECIMAL | SQL_CVT_BIT | SQL_CVT_TINYINT | SQL_CVT_SMALLINT |
                    SQL_CVT_INTEGER | SQL_CVT_BIGINT | SQL_CVT_REAL | SQL_CVT_FLOAT | SQL_CVT_DOUBLE |
                    SQL_CVT_BINARY | SQL_CVT_VARBINARY | SQL_CVT_LONGVARBINARY | SQL_CVT_DATE |
                    SQL_CVT_TIME | SQL_CVT_TIMESTAMP | SQL_CVT_GUID;
#endif //SQL_CONVERT_LONGVARBINARY

#ifdef SQL_CONVERT_GUID
                // Bitmask indicates the conversions supported by the CONVERT scalar function for target type GUID
                intParams[SQL_CONVERT_GUID] = SQL_CVT_CHAR | SQL_CVT_VARCHAR | SQL_CVT_LONGVARCHAR |
                    SQL_CVT_WCHAR | SQL_CVT_WLONGVARCHAR | SQL_CVT_WVARCHAR |
                    SQL_CVT_BINARY | SQL_CVT_VARBINARY | SQL_CVT_LONGVARBINARY | SQL_CVT_GUID;
#endif //SQL_CONVERT_GUID

                //======================= Short Params ========================
#ifdef SQL_MAX_CONCURRENT_ACTIVITIES
                // The maximum number of active statements that the driver can
                // support for a connection. Zero mean no limit.
                shortParams[SQL_MAX_CONCURRENT_ACTIVITIES] = 32;
#endif // SQL_MAX_CONCURRENT_ACTIVITIES

#ifdef SQL_CURSOR_COMMIT_BEHAVIOR
                // Indicates how a COMMIT operation affects cursors and prepared
                // statements in the data source.
                shortParams[SQL_CURSOR_COMMIT_BEHAVIOR] = SQL_CB_PRESERVE;
#endif // SQL_CURSOR_COMMIT_BEHAVIOR

#ifdef SQL_CURSOR_ROLLBACK_BEHAVIOR
                // Indicates how a ROLLBACK  operation affects cursors and prepared
                // statements in the data source.
                shortParams[SQL_CURSOR_ROLLBACK_BEHAVIOR] = SQL_CB_PRESERVE;
#endif // SQL_CURSOR_ROLLBACK_BEHAVIOR

#ifdef SQL_TXN_CAPABLE
                // Describs the transaction support in the driver or data source.
                shortParams[SQL_TXN_CAPABLE] = SQL_TC_NONE;
#endif // SQL_TXN_CAPABLE

#ifdef SQL_QUOTED_IDENTIFIER_CASE
                // Case-sensitiveness of the quoted identifiers in SQL.
                shortParams[SQL_QUOTED_IDENTIFIER_CASE] = SQL_IC_SENSITIVE;
#endif // SQL_QUOTED_IDENTIFIER_CASE
            }

            ConnectionInfo::~ConnectionInfo()
            {
                // No-op.
            }

            SqlResult ConnectionInfo::GetInfo(InfoType type, void* buf,
                short buflen, short* reslen) const
            {
                if (!buf || !buflen)
                    return SQL_RESULT_ERROR;

                StringInfoMap::const_iterator itStr = strParams.find(type);

                if (itStr != strParams.end())
                {
                    unsigned short strlen = static_cast<short>(
                        utility::CopyStringToBuffer(itStr->second,
                            reinterpret_cast<char*>(buf), buflen));

                    if (reslen)
                        *reslen = strlen;

                    return SQL_RESULT_SUCCESS;
                }

                UintInfoMap::const_iterator itInt = intParams.find(type);

                if (itInt != intParams.end())
                {
                    unsigned int *res = reinterpret_cast<unsigned int*>(buf);

                    *res = itInt->second;

                    return SQL_RESULT_SUCCESS;
                }

                UshortInfoMap::const_iterator itShort = shortParams.find(type);

                if (itShort != shortParams.end())
                {
                    unsigned short *res = reinterpret_cast<unsigned short*>(buf);

                    *res = itShort->second;

                    return SQL_RESULT_SUCCESS;
                }

                return SQL_RESULT_ERROR;
            }
        }
    }
}

