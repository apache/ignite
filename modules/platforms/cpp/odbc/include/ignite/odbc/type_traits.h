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

#ifndef _IGNITE_ODBC_TYPE_TRAITS
#define _IGNITE_ODBC_TYPE_TRAITS

#include <stdint.h>

#include <string>

namespace ignite
{
    namespace odbc
    {
        namespace type_traits
        {
#ifdef _DEBUG
            /**
             * Convert statement attribute ID to string containing its name.
             * Debug function.
             * @param id Attribute ID.
             * @return Null-terminated string containing attribute name.
             */
            const char* StatementAttrIdToString(long id);
#endif //_DEBUG

            /** 
             * ODBC type aliases.
             * We use these so we will not be needed to include system-specific
             * headers in our header files.
             */
            struct OdbcNativeType
            {
                enum Type
                {
                    /** Alias for the SQL_C_CHAR type. */
                    AI_CHAR,

                    /** Alias for the SQL_C_WCHAR type. */
                    AI_WCHAR,

                    /** Alias for the SQL_C_SSHORT type. */
                    AI_SIGNED_SHORT,

                    /** Alias for the SQL_C_USHORT type. */
                    AI_UNSIGNED_SHORT,

                    /** Alias for the SQL_C_SLONG type. */
                    AI_SIGNED_LONG,

                    /** Alias for the SQL_C_ULONG type. */
                    AI_UNSIGNED_LONG,

                    /** Alias for the SQL_C_FLOAT type. */
                    AI_FLOAT,

                    /** Alias for the SQL_C_DOUBLE type. */
                    AI_DOUBLE,

                    /** Alias for the SQL_C_BIT type. */
                    AI_BIT,

                    /** Alias for the SQL_C_STINYINT type. */
                    AI_SIGNED_TINYINT,

                    /** Alias for the SQL_C_UTINYINT type. */
                    AI_UNSIGNED_TINYINT,

                    /** Alias for the SQL_C_SBIGINT type. */
                    AI_SIGNED_BIGINT,

                    /** Alias for the SQL_C_UBIGINT type. */
                    AI_UNSIGNED_BIGINT,

                    /** Alias for the SQL_C_BINARY type. */
                    AI_BINARY,

                    /** Alias for the SQL_C_TDATE type. */
                    AI_TDATE,

                    /** Alias for the SQL_C_TTIME type. */
                    AI_TTIME,

                    /** Alias for the SQL_C_TTIMESTAMP type. */
                    AI_TTIMESTAMP,

                    /** Alias for the SQL_C_NUMERIC type. */
                    AI_NUMERIC,

                    /** Alias for the SQL_C_GUID type. */
                    AI_GUID,

                    /** Alias for the SQL_DEFAULT. */
                    AI_DEFAULT,

                    /** Alias for all unsupported types. */
                    AI_UNSUPPORTED
                };
            };

            /**
             * SQL type name constants.
             */
            class SqlTypeName
            {
            public:
                /** VARCHAR SQL type name constant. */
                static const std::string VARCHAR;

                /** SMALLINT SQL type name constant. */
                static const std::string SMALLINT;

                /** INTEGER SQL type name constant. */
                static const std::string INTEGER;

                /** DECIMAL SQL type name constant. */
                static const std::string DECIMAL;

                /** FLOAT SQL type name constant. */
                static const std::string FLOAT;

                /** DOUBLE SQL type name constant. */
                static const std::string DOUBLE;

                /** BIT SQL type name constant. */
                static const std::string BIT;

                /** TINYINT SQL type name constant. */
                static const std::string TINYINT;

                /** BIGINT SQL type name constant. */
                static const std::string BIGINT;

                /** BINARY SQL type name constant. */
                static const std::string BINARY;

                /** DATE SQL type name constant. */
                static const std::string DATE;

                /** TIMESTAMP SQL type name constant. */
                static const std::string TIMESTAMP;

                /** TIME SQL type name constant. */
                static const std::string TIME;

                /** GUID SQL type name constant. */
                static const std::string GUID;
            };

            /**
             * Get SQL type name for the binary type.
             *
             * @param binaryType Binary type.
             * @return Corresponding SQL type name.
             */
            const std::string& BinaryTypeToSqlTypeName(int8_t binaryType);

            /**
             * Check if the C type supported by the current implementation.
             *
             * @param type Application type.
             * @return True if the type is supported.
             */
            bool IsApplicationTypeSupported(int16_t type);

            /**
             * Check if the SQL type supported by the current implementation.
             *
             * @param type Application type.
             * @return True if the type is supported.
             */
            bool IsSqlTypeSupported(int16_t type);

            /**
             * Get corresponding binary type for ODBC SQL type.
             *
             * @param sqlType SQL type.
             * @return Binary type.
             */
            int8_t SqlTypeToBinary(int16_t sqlType);

            /**
             * Convert ODBC type to driver type alias.
             *
             * @param type ODBC type;
             * @return Internal driver type.
             */
            OdbcNativeType::Type ToDriverType(int16_t type);

            /**
             * Convert binary data type to SQL data type.
             *
             * @param binaryType Binary data type.
             * @return SQL data type.
             */
            int16_t BinaryToSqlType(int8_t binaryType);

            /**
             * Get binary type SQL nullability.
             *
             * @param binaryType Binary data type.
             * @return SQL_NO_NULLS if the column could not include NULL values.
             *         SQL_NULLABLE if the column accepts NULL values.
             *         SQL_NULLABLE_UNKNOWN if it is not known whether the 
             *         column accepts NULL values.
             */
            int16_t BinaryTypeNullability(int8_t binaryType);

            /**
             * Get SQL type display size.
             *
             * @param type SQL type.
             * @return Display size.
             */
            int32_t SqlTypeDisplaySize(int16_t type);

            /**
             * Get binary type display size.
             *
             * @param type Binary type.
             * @return Display size.
             */
            int32_t BinaryTypeDisplaySize(int8_t type);

            /**
             * Get SQL type column size.
             *
             * @param type SQL type.
             * @return Column size.
             */
            int32_t SqlTypeColumnSize(int16_t type);

            /**
             * Get binary type column size.
             *
             * @param type Binary type.
             * @return Column size.
             */
            int32_t BinaryTypeColumnSize(int8_t type);

            /**
             * Get SQL type transfer octet length.
             *
             * @param type SQL type.
             * @return Transfer octet length.
             */
            int32_t SqlTypeTransferLength(int16_t type);

            /**
             * Get binary type transfer octet length.
             *
             * @param type Binary type.
             * @return Transfer octet length.
             */
            int32_t BinaryTypeTransferLength(int8_t type);

            /**
             * Get SQL type numeric precision radix.
             *
             * @param type SQL type.
             * @return Numeric precision radix.
             */
            int32_t SqlTypeNumPrecRadix(int8_t type);

            /**
             * Get binary type numeric precision radix.
             *
             * @param type Binary type.
             * @return Numeric precision radix.
             */
            int32_t BinaryTypeNumPrecRadix(int8_t type);

            /**
             * Get SQL type decimal digits.
             *
             * @param type SQL type.
             * @return Decimal digits.
             */
            int32_t SqlTypeDecimalDigits(int16_t type);

            /**
             * Get binary type decimal digits.
             *
             * @param type Binary type.
             * @return Decimal digits.
             */
            int32_t BinaryTypeDecimalDigits(int8_t type);

            /**
             * Checks if the SQL type is unsigned.
             *
             * @param type SQL type.
             * @return True if unsigned or non-numeric.
             */
            bool SqlTypeUnsigned(int16_t type);

            /**
             * Checks if the binary type is unsigned.
             *
             * @param type Binary type.
             * @return True if unsigned or non-numeric.
             */
            bool BinaryTypeUnsigned(int8_t type);
        }
    }
}

#endif //_IGNITE_ODBC_TYPE_TRAITS
