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

#ifdef _WIN32
#   define _WINSOCKAPI_
#   include <windows.h>

    // Undefining windows macro to use standard library tool
#   undef min
#endif //_WIN32

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


#include <sqlext.h>
#include <odbcinst.h>

#include <cstring>
#include <algorithm>

#include "utility.h"
#include "connection_info.h"

namespace ignite
{
    namespace odbc
    {

#ifdef _DEBUG

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
                DBG_STR_CASE(SQL_ASYNC_DBC_FUNCTIONS);
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
            default: 
                break;
            }
            return "<< UNKNOWN TYPE >>";
        }

#undef DBG_STR_CASE
#endif

        ConnectionInfo::ConnectionInfo() : strParams(), intParams(),
            shortParams()
        {
            //========================= String Params =========================
            // Driver name.
            strParams[SQL_DRIVER_NAME] = "Apache Ignite";
            strParams[SQL_DBMS_NAME]   = "Apache Ignite";

            // ODBC version.
            strParams[SQL_DRIVER_ODBC_VER] = "02.00";
            strParams[SQL_DBMS_VER]        = "02.00";

            // Driver version. At a minimum, the version is of the form 
            // ##.##.####, where the first two digits are the major version,
            // the next two digits are the minor version, and the last four
            // digits are the release version.
            strParams[SQL_DRIVER_VER] = "01.05.0000";

            // A character string: "Y" if the data source supports column 
            // aliases; otherwise, "N".
            strParams[SQL_COLUMN_ALIAS] = "N";

            // The character string that is used as the starting and ending
            // delimiter of a quoted (delimited) identifier in SQL statements.
            // Identifiers passed as arguments to ODBC functions do not have to
            // be quoted. If the data source does not support quoted
            // identifiers, a blank is returned.
            strParams[SQL_IDENTIFIER_QUOTE_CHAR] = "\"";

            // A character string: the character or characters that the data
            // source defines as the separator between a catalog name and the
            // qualified name element that follows or precedes it.
            strParams[SQL_CATALOG_NAME_SEPARATOR] = ".";

            // A character string that contains all special characters (that
            // is, all characters except a through z, A through Z, 0 through 9,
            // and underscore) that can be used in an identifier name, such as
            // a table name, column name, or index name, on the data source.
            strParams[SQL_SPECIAL_CHARACTERS] = "";

            // A character string with the data source vendor's name for
            // a catalog; for example, "database" or "directory". This string
            // can be in upper, lower, or mixed case.
            strParams[SQL_CATALOG_TERM] = "catalog";

            // A character string with the data source vendor's name for
            // a table; for example, "table" or "file".
            strParams[SQL_TABLE_TERM] = "table";

            // A character string with the data source vendor's name for 
            // a schema; for example, "owner", "Authorization ID", or "Schema".
            strParams[SQL_SCHEMA_TERM] = "schema";

            //======================== Integer Params =========================
            // Indicates if the driver can execute functions asynchronously
            // on the connection handle.
            // SQL_ASYNC_DBC_CAPABLE = The driver can execute connection
            // functions asynchronously.
            // SQL_ASYNC_DBC_NOT_CAPABLE = The driver can not execute
            // connection functions asynchronously.
            intParams[SQL_ASYNC_DBC_FUNCTIONS] = SQL_ASYNC_DBC_NOT_CAPABLE;

            // Indicates if the driver supports asynchronous notification.
            // SQL_ASYNC_NOTIFICATION_CAPABLE  = Asynchronous execution 
            // notification is supported by the driver.
            // SQL_ASYNC_NOTIFICATION_NOT_CAPABLE Asynchronous execution 
            // notification is not supported by the driver.
            intParams[SQL_ASYNC_NOTIFICATION] = SQL_ASYNC_NOTIFICATION_NOT_CAPABLE;

            // Bitmask enumerating extensions to SQLGetData.
            intParams[SQL_GETDATA_EXTENSIONS] = SQL_GD_ANY_COLUMN;

            // Indicates the level of the ODBC 3.x interface that the driver 
            // complies with.
            intParams[SQL_ODBC_INTERFACE_CONFORMANCE] = SQL_OIC_CORE;

            // Indicates the level of SQL-92 supported by the driver.
            intParams[SQL_SQL_CONFORMANCE] = 0; // SQL_SC_SQL92_ENTRY;

            // Bitmask enumerating the statements in which catalogs can be used.
            intParams[SQL_CATALOG_USAGE] = 0;

            // Bitmask enumerating the statements in which schemas can be used.
            intParams[SQL_SCHEMA_USAGE] = 0;

            // Indicates the maximum size in characters that the data source 
            // supports for user-defined names.
            intParams[SQL_MAX_IDENTIFIER_LEN] = 128;

            // Bitmask enumerating support for aggregation functions.
            intParams[SQL_AGGREGATE_FUNCTIONS] = SQL_AF_ALL | SQL_AF_AVG | 
                SQL_AF_COUNT | SQL_AF_DISTINCT | SQL_AF_MAX | SQL_AF_MIN |
                SQL_AF_SUM;

            // Bitmask enumerating the scalar numeric functions supported by
            // the driver and associated data source.
            intParams[SQL_NUMERIC_FUNCTIONS] = SQL_FN_NUM_ABS;

            // Bitmask enumerating the scalar string functions supported by the
            // driver and associated data source.
            intParams[SQL_STRING_FUNCTIONS] = 0;

            // Bitmask enumerating the scalar date and time functions supported
            // by the driver and associated data source.
            intParams[SQL_TIMEDATE_FUNCTIONS] = 0;

            // Bitmask enumerating timestamp intervals supported by the driver 
            // and associated data source for the TIMESTAMPADD scalar function.
            intParams[SQL_TIMEDATE_ADD_INTERVALS] = 0;

            // Bitmask enumerating timestamp intervals supported by the driver
            // and associated data source for the TIMESTAMPDIFF scalar function.
            intParams[SQL_TIMEDATE_DIFF_INTERVALS] = 0;

            // Bitmask enumerating the SQL-92 datetime literals supported by
            // the data source.
            intParams[SQL_DATETIME_LITERALS] = 0;

            // Bitmask enumerating the scalar system functions supported by the
            // driver and associated data source.
            intParams[SQL_SYSTEM_FUNCTIONS] = 0;

            // Bitmask enumerating the scalar conversion functions supported
            // by the driver and associated data source.
            intParams[SQL_CONVERT_FUNCTIONS] = 0;

            // Bitmask enumerating the types of outer joins supported by the 
            // driver and data source.
            intParams[SQL_OJ_CAPABILITIES] = SQL_OJ_LEFT | SQL_OJ_RIGHT |
                SQL_OJ_FULL | SQL_OJ_NESTED | SQL_OJ_INNER | 
                SQL_OJ_ALL_COMPARISON_OPS;

            // Bitmask enumerating the support operations in SQLSetPos.
            intParams[SQL_POS_OPERATIONS] = 0;

            //========================= Short Params ==========================
            // The maximum number of active statements that the driver can
            // support for a connection. Zero mean no limit.
            shortParams[SQL_MAX_CONCURRENT_ACTIVITIES] = 32;

            // Indicates how a COMMIT operation affects cursors and prepared
            // statements in the data source.
            shortParams[SQL_CURSOR_COMMIT_BEHAVIOR] = SQL_CB_PRESERVE;

            // Indicates how a ROLLBACK  operation affects cursors and prepared
            // statements in the data source.
            shortParams[SQL_CURSOR_ROLLBACK_BEHAVIOR] = SQL_CB_PRESERVE;

            // Describs the transaction support in the driver or data source.
            shortParams[SQL_TXN_CAPABLE] = SQL_TC_NONE;

            // Case-sensitiveness of the quoted identifiers in SQL.
            shortParams[SQL_QUOTED_IDENTIFIER_CASE] = SQL_IC_SENSITIVE;
        }

        ConnectionInfo::~ConnectionInfo()
        {
            // No-op.
        }

        bool ConnectionInfo::GetInfo(InfoType type, void* buf,
            short buflen, short* reslen) const
        {
            if (!buf || !buflen)
                return false;

            StringInfoMap::const_iterator itStr = strParams.find(type);

            if (itStr != strParams.cend()) 
            {
                unsigned short strlen = static_cast<short>(
                    utility::CopyStringToBuffer(itStr->second, 
                        reinterpret_cast<char*>(buf), buflen));

                if (reslen)
                    *reslen = strlen;

                return true;
            }

            UintInfoMap::const_iterator itInt = intParams.find(type);

            if (itInt != intParams.cend())
            {
                unsigned int *res = reinterpret_cast<unsigned int*>(buf);

                *res = itInt->second;

                return true;
            }

            UshortInfoMap::const_iterator itShort = shortParams.find(type);

            if (itShort != shortParams.cend())
            {
                unsigned short *res = reinterpret_cast<unsigned short*>(buf);

                *res = itShort->second;

                return true;
            }

            return false;
        }
    }
}

