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

#include <algorithm>
#include <string>
#include <sstream>

#include "ignite/odbc/system/odbc_constants.h"
#include "ignite/odbc/app/parameter.h"
#include "ignite/odbc/utility.h"

namespace ignite
{
    namespace odbc
    {
        namespace app
        {
            Parameter::Parameter() :
                buffer(),
                sqlType(),
                columnSize(),
                decDigits()
            {
                // No-op.
            }

            Parameter::Parameter(const ApplicationDataBuffer& buffer, int16_t sqlType, 
                size_t columnSize, int16_t decDigits) :
                buffer(buffer),
                sqlType(sqlType),
                columnSize(columnSize),
                decDigits(decDigits)
            {
                // No-op.
            }

            Parameter::Parameter(const Parameter & other) :
                buffer(other.buffer),
                sqlType(other.sqlType),
                columnSize(other.columnSize),
                decDigits(other.decDigits)
            {
                // No-op.
            }

            Parameter::~Parameter()
            {
                // No-op.
            }

            Parameter& Parameter::operator=(const Parameter &other)
            {
                buffer = other.buffer;
                sqlType = other.sqlType;
                columnSize = other.columnSize;
                decDigits = other.decDigits;

                return *this;
            }

            void Parameter::Write(ignite::impl::binary::BinaryWriterImpl& writer) const
            {
                switch (sqlType)
                {
                    case SQL_CHAR:
                    case SQL_VARCHAR:
                    case SQL_LONGVARCHAR:
                    {
                        utility::WriteString(writer, buffer.GetString(columnSize));
                        break;
                    }

                    case SQL_SMALLINT:
                    {
                        writer.WriteInt16(buffer.GetInt16());
                        break;
                    }

                    case SQL_INTEGER:
                    {
                        writer.WriteInt32(buffer.GetInt32());
                        break;
                    }

                    case SQL_FLOAT:
                    {
                        writer.WriteFloat(buffer.GetFloat());
                        break;
                    }

                    case SQL_DOUBLE:
                    {
                        writer.WriteDouble(buffer.GetDouble());
                        break;
                    }

                    case SQL_TINYINT:
                    {
                        writer.WriteInt8(buffer.GetInt8());
                        break;
                    }

                    case SQL_BIT:
                    {
                        writer.WriteBool(buffer.GetInt8() != 0);
                        break;
                    }

                    case SQL_BIGINT:
                    {
                        writer.WriteInt64(buffer.GetInt64());
                        break;
                    }

                    case SQL_DATE:
                    {
                        writer.WriteDate(buffer.GetDate());
                        break;
                    }

                    case SQL_TIMESTAMP:
                    {
                        writer.WriteTimestamp(buffer.GetTimestamp());
                        break;
                    }

                    case SQL_BINARY:
                    case SQL_VARBINARY:
                    case SQL_LONGVARBINARY:
                    {
                        writer.WriteInt8Array(reinterpret_cast<const int8_t*>(buffer.GetData()),
                                              static_cast<int32_t>(buffer.GetSize()));
                        break;
                    }

                    case SQL_GUID:
                    {
                        writer.WriteGuid(buffer.GetGuid());

                        break;
                    }

                    case SQL_DECIMAL:
                    {
                        common::Decimal dec;
                        buffer.GetDecimal(dec);

                        utility::WriteDecimal(writer, dec);

                        break;
                    }

                    default:
                        break;
                }
            }

            ApplicationDataBuffer & Parameter::GetBuffer()
            {
                return buffer;
            }
        }
    }
}

