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
                decDigits(),
                nullData(false),
                storedData()
            {
                // No-op.
            }

            Parameter::Parameter(const ApplicationDataBuffer& buffer, int16_t sqlType,
                size_t columnSize, int16_t decDigits) :
                buffer(buffer),
                sqlType(sqlType),
                columnSize(columnSize),
                decDigits(decDigits),
                nullData(false),
                storedData()
            {
                // No-op.
            }

            Parameter::Parameter(const Parameter& other) :
                buffer(other.buffer),
                sqlType(other.sqlType),
                columnSize(other.columnSize),
                decDigits(other.decDigits),
                nullData(other.nullData),
                storedData(other.storedData)
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
                if (buffer.GetInputSize() == SQL_NULL_DATA)
                {
                    writer.WriteNull();

                    return;
                }

                // Buffer to use to get data.
                ApplicationDataBuffer buf(buffer);

                SqlLen storedDataLen = static_cast<SqlLen>(storedData.size());

                if (buffer.IsDataAtExec())
                {
                    buf = ApplicationDataBuffer(buffer.GetType(),
                        const_cast<int8_t*>(&storedData[0]), storedDataLen, &storedDataLen);
                }

                switch (sqlType)
                {
                    case SQL_CHAR:
                    case SQL_VARCHAR:
                    case SQL_LONGVARCHAR:
                    {
                        utility::WriteString(writer, buf.GetString(columnSize));
                        break;
                    }

                    case SQL_SMALLINT:
                    {
                        writer.WriteObject<int16_t>(buf.GetInt16());
                        break;
                    }

                    case SQL_INTEGER:
                    {
                        writer.WriteObject<int32_t>(buf.GetInt32());
                        break;
                    }

                    case SQL_FLOAT:
                    {
                        writer.WriteObject<float>(buf.GetFloat());
                        break;
                    }

                    case SQL_DOUBLE:
                    {
                        writer.WriteObject<double>(buf.GetDouble());
                        break;
                    }

                    case SQL_TINYINT:
                    {
                        writer.WriteObject<int8_t>(buf.GetInt8());
                        break;
                    }

                    case SQL_BIT:
                    {
                        writer.WriteObject<bool>(buf.GetInt8() != 0);
                        break;
                    }

                    case SQL_BIGINT:
                    {
                        writer.WriteObject<int64_t>(buf.GetInt64());
                        break;
                    }

                    case SQL_TYPE_DATE:
                    case SQL_DATE:
                    {
                        writer.WriteDate(buf.GetDate());
                        break;
                    }

                    case SQL_TYPE_TIMESTAMP:
                    case SQL_TIMESTAMP:
                    {
                        writer.WriteTimestamp(buf.GetTimestamp());
                        break;
                    }

                    case SQL_BINARY:
                    case SQL_VARBINARY:
                    case SQL_LONGVARBINARY:
                    {
                        const ApplicationDataBuffer& constRef = buf;

                        const SqlLen* resLenPtr = constRef.GetResLen();

                        if (!resLenPtr)
                            break;

                        int32_t paramLen = static_cast<int32_t>(*resLenPtr);

                        writer.WriteInt8Array(reinterpret_cast<const int8_t*>(constRef.GetData()), paramLen);

                        break;
                    }

                    case SQL_GUID:
                    {
                        writer.WriteGuid(buf.GetGuid());

                        break;
                    }

                    case SQL_DECIMAL:
                    {
                        common::Decimal dec;
                        buf.GetDecimal(dec);

                        utility::WriteDecimal(writer, dec);

                        break;
                    }

                    default:
                        break;
                }
            }

            ApplicationDataBuffer& Parameter::GetBuffer()
            {
                return buffer;
            }

            void Parameter::ResetStoredData()
            {
                storedData.clear();

                if (buffer.IsDataAtExec())
                    storedData.reserve(buffer.GetDataAtExecSize());
            }

            bool Parameter::IsDataReady() const
            {
                return !buffer.IsDataAtExec() ||
                       storedData.size() == buffer.GetDataAtExecSize();
            }

            void Parameter::PutData(void* data, SqlLen len)
            {
                if (len == SQL_DEFAULT_PARAM)
                    return;

                if (len == SQL_NULL_DATA)
                {
                    nullData = true;

                    return;
                }

                if (buffer.GetType() == type_traits::IGNITE_ODBC_C_TYPE_CHAR ||
                    buffer.GetType() == type_traits::IGNITE_ODBC_C_TYPE_BINARY)
                {
                    SqlLen slen = len;

                    if (buffer.GetType() == type_traits::IGNITE_ODBC_C_TYPE_CHAR && slen == SQL_NTSL)
                    {
                        const char* str = reinterpret_cast<char*>(data);

                        slen = strlen(str);
                    }

                    if (slen <= 0)
                        return;

                    size_t beginPos = storedData.size();

                    storedData.resize(storedData.size() + static_cast<size_t>(slen));

                    memcpy(&storedData[beginPos], data, static_cast<size_t>(slen));

                    return;
                }

                size_t dataSize = buffer.GetDataAtExecSize();

                storedData.resize(dataSize);

                memcpy(&storedData[0], data, dataSize);
            }
        }
    }
}
