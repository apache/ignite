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

#   undef min
#endif //_WIN32

#include <sqlext.h>
#include <odbcinst.h>

#include <algorithm>
#include <string>
#include <sstream>

#include "ignite/odbc/app/application_data_buffer.h"

namespace ignite
{
    namespace odbc
    {
        namespace app
        {
            ApplicationDataBuffer::ApplicationDataBuffer() :
                type(type_traits::IGNITE_ODBC_C_TYPE_UNSUPPORTED), buffer(0), buflen(0), reslen(0)
            {
                // No-op.
            }

            ApplicationDataBuffer::ApplicationDataBuffer(type_traits::IgniteSqlType type, 
                void* bufferPtr, int64_t buflen, int64_t* reslen) :
                type(type), buffer(bufferPtr), buflen(buflen), reslen(reslen)
            {
                // No-op.
            }

            ApplicationDataBuffer::ApplicationDataBuffer(const ApplicationDataBuffer & other) :
                type(other.type), buffer(other.buffer), buflen(other.buflen), reslen(other.reslen)
            {
                // No-op.
            }

            ApplicationDataBuffer::~ApplicationDataBuffer()
            {
                // No-op.
            }

            ApplicationDataBuffer & ApplicationDataBuffer::operator=(const ApplicationDataBuffer & other)
            {
                type = other.type;
                buffer = other.buffer;
                buflen = other.buflen;
                reslen = other.reslen;

                return *this;
            }

            template<typename T>
            void ApplicationDataBuffer::PutNum(T value)
            {
                using namespace type_traits;
                switch (type)
                {
                    case IGNITE_ODBC_C_TYPE_SIGNED_TINYINT:
                    {
                        PutNumToNumBuffer<signed char>(value);
                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_BIT:
                    case IGNITE_ODBC_C_TYPE_UNSIGNED_TINYINT:
                    {
                        PutNumToNumBuffer<unsigned char>(value);
                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_SIGNED_SHORT:
                    {
                        PutNumToNumBuffer<short>(value);
                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_UNSIGNED_SHORT:
                    {
                        PutNumToNumBuffer<unsigned short>(value);
                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_SIGNED_LONG:
                    {
                        PutNumToNumBuffer<long>(value);
                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_UNSIGNED_LONG:
                    {
                        PutNumToNumBuffer<unsigned long>(value);
                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_SIGNED_BIGINT:
                    {
                        PutNumToNumBuffer<int64_t>(value);
                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_UNSIGNED_BIGINT:
                    {
                        PutNumToNumBuffer<uint64_t>(value);
                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_FLOAT:
                    {
                        PutNumToNumBuffer<float>(value);
                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_DOUBLE:
                    {
                        PutNumToNumBuffer<double>(value);
                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_CHAR:
                    {
                        PutValToStrBuffer<char>(value);
                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_WCHAR:
                    {
                        PutValToStrBuffer<wchar_t>(value);
                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_NUMERIC:
                    {
                        if (buffer)
                        {
                            SQL_NUMERIC_STRUCT* out =
                                reinterpret_cast<SQL_NUMERIC_STRUCT*>(buffer);

                            out->precision = 0;
                            out->scale = 0;
                            out->sign = value > 0 ? 1 : 0;

                            // TODO: implement propper conversation to numeric type.
                            int64_t intVal = static_cast<int64_t>(std::abs(value));

                            memcpy(out->val, &intVal, std::min<int>(SQL_MAX_NUMERIC_LEN, sizeof(intVal)));
                        }
                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_BINARY:
                    case IGNITE_ODBC_C_TYPE_DEFAULT:
                    {
                        if (buffer)
                        {
                            if (buflen >= sizeof(value))
                            {
                                memcpy(buffer, &value, sizeof(value));

                                if (reslen)
                                    *reslen = sizeof(value);
                            }
                            else
                            {
                                memcpy(buffer, &value, buflen);

                                if (reslen)
                                    *reslen = SQL_NO_TOTAL;
                            }
                        }
                        else if (reslen)
                        {
                            *reslen = sizeof(value);
                        }
                        break;
                    }

                    default:
                    {
                        if (reslen)
                            *reslen = SQL_NO_TOTAL;
                    }
                }
            }

            template<typename Tbuf, typename Tin>
            void ApplicationDataBuffer::PutNumToNumBuffer(Tin value)
            {
                if (buffer)
                {
                    Tbuf* out = reinterpret_cast<Tbuf*>(buffer);
                    *out = static_cast<Tbuf>(value);
                }
            }

            template<typename CharT, typename Tin>
            void ApplicationDataBuffer::PutValToStrBuffer(const Tin & value)
            {
                typedef std::basic_stringstream<CharT> ConverterType;

                ConverterType converter;

                converter << value;

                PutStrToStrBuffer<CharT>(converter.str());
            }

            template<typename CharT>
            void ApplicationDataBuffer::PutValToStrBuffer(const int8_t & value)
            {
                typedef std::basic_stringstream<CharT> ConverterType;

                ConverterType converter;

                converter << static_cast<int>(value);

                PutStrToStrBuffer<CharT>(converter.str());
            }

            template<typename OutCharT, typename InCharT>
            void ApplicationDataBuffer::PutStrToStrBuffer(const std::basic_string<InCharT>& value)
            {
                int64_t charSize = static_cast<int64_t>(sizeof(OutCharT));

                if (buffer)
                {
                    if (buflen >= charSize)
                    {
                        OutCharT* out = reinterpret_cast<OutCharT*>(buffer);

                        int64_t outLen = (buflen / charSize) - 1;

                        int64_t toCopy = std::min<int64_t>(outLen, value.size());

                        for (int64_t i = 0; i < toCopy; ++i)
                            out[i] = value[i];

                        out[toCopy] = 0;
                    }

                    if (*reslen)
                    {
                        if (buflen >= static_cast<int64_t>((value.size() + 1) * charSize))
                            *reslen = value.size();
                        else
                            *reslen = SQL_NO_TOTAL;
                    }
                }
                else if (reslen)
                {
                    *reslen = value.size();
                }
            }

            void ApplicationDataBuffer::PutRawDataToBuffer(void *data, size_t len)
            {
                int64_t ilen = static_cast<int64_t>(len);

                if (buffer)
                {
                    int64_t toCopy = std::min(buflen, ilen);

                    memcpy(buffer, data, toCopy);

                    if (*reslen)
                    {
                        if (buflen >= ilen)
                            *reslen = ilen;
                        else
                            *reslen = SQL_NO_TOTAL;
                    }
                }
                else if (reslen)
                {
                    *reslen = ilen;
                }
            }

            void ApplicationDataBuffer::PutInt8(int8_t value)
            {
                PutNum(value);
            }

            void ApplicationDataBuffer::PutInt16(int16_t value)
            {
                PutNum(value);
            }

            void ApplicationDataBuffer::PutInt32(int32_t value)
            {
                PutNum(value);
            }

            void ApplicationDataBuffer::PutInt64(int64_t value)
            {
                PutNum(value);
            }

            void ApplicationDataBuffer::PutFloat(float value)
            {
                PutNum(value);
            }

            void ApplicationDataBuffer::PutDouble(double value)
            {
                PutNum(value);
            }

            void ApplicationDataBuffer::PutString(const std::string & value)
            {
                using namespace type_traits;
                switch (type)
                {
                    case IGNITE_ODBC_C_TYPE_SIGNED_TINYINT:
                    case IGNITE_ODBC_C_TYPE_BIT:
                    case IGNITE_ODBC_C_TYPE_UNSIGNED_TINYINT:
                    case IGNITE_ODBC_C_TYPE_SIGNED_SHORT:
                    case IGNITE_ODBC_C_TYPE_UNSIGNED_SHORT:
                    case IGNITE_ODBC_C_TYPE_SIGNED_LONG:
                    case IGNITE_ODBC_C_TYPE_UNSIGNED_LONG:
                    case IGNITE_ODBC_C_TYPE_SIGNED_BIGINT:
                    case IGNITE_ODBC_C_TYPE_UNSIGNED_BIGINT:
                    case IGNITE_ODBC_C_TYPE_NUMERIC:
                    {
                        std::stringstream converter(value);

                        int64_t numValue;

                        converter >> numValue;

                        PutNum(numValue);

                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_FLOAT:
                    case IGNITE_ODBC_C_TYPE_DOUBLE:
                    {
                        std::stringstream converter(value);

                        double numValue;

                        converter >> numValue;

                        PutNum(numValue);

                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_CHAR:
                    case IGNITE_ODBC_C_TYPE_BINARY:
                    case IGNITE_ODBC_C_TYPE_DEFAULT:
                    {
                        PutStrToStrBuffer<char>(value);

                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_WCHAR:
                    {
                        PutStrToStrBuffer<wchar_t>(value);

                        break;
                    }

                    default:
                    {
                        if (reslen)
                            *reslen = SQL_NO_TOTAL;
                    }
                }
            }

            void ApplicationDataBuffer::PutGuid(const Guid & value)
            {
                using namespace type_traits;
                switch (type)
                {
                    case IGNITE_ODBC_C_TYPE_CHAR:
                    case IGNITE_ODBC_C_TYPE_BINARY:
                    case IGNITE_ODBC_C_TYPE_DEFAULT:
                    {
                        PutValToStrBuffer<char>(value);
                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_WCHAR:
                    {
                        PutValToStrBuffer<wchar_t>(value);
                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_GUID:
                    {
                        SQLGUID* guid = reinterpret_cast<SQLGUID*>(buffer);

                        guid->Data1 = static_cast<uint32_t>(value.GetMostSignificantBits() >> 32);
                        guid->Data2 = static_cast<uint16_t>(value.GetMostSignificantBits() >> 16);
                        guid->Data3 = static_cast<uint16_t>(value.GetMostSignificantBits());

                        uint64_t lsb = value.GetLeastSignificantBits();
                        for (size_t i = 0; i < sizeof(guid->Data4); ++i)
                            guid->Data4[i] = (lsb >> (sizeof(guid->Data4) - i - 1) * 8) & 0xFF;

                        break;
                    }

                    default:
                    {
                        if (reslen)
                            *reslen = SQL_NO_TOTAL;
                    }
                }
            }

            void ApplicationDataBuffer::PutBinaryData(void *data, size_t len)
            {
                using namespace type_traits;
                switch (type)
                {
                    case IGNITE_ODBC_C_TYPE_CHAR:
                    case IGNITE_ODBC_C_TYPE_WCHAR:
                    case IGNITE_ODBC_C_TYPE_BINARY:
                    case IGNITE_ODBC_C_TYPE_DEFAULT:
                    {
                        PutRawDataToBuffer(data, len);
                        break;
                    }

                    default:
                    {
                        if (reslen)
                            *reslen = SQL_NO_TOTAL;
                    }
                }
            }

            void ApplicationDataBuffer::PutNull()
            {
                if (reslen)
                    *reslen = SQL_NULL_DATA;
            }

            std::string ApplicationDataBuffer::GetString(size_t maxLen) const
            {
                using namespace type_traits;
                std::string res;

                switch (type)
                {
                    case IGNITE_ODBC_C_TYPE_CHAR:
                    {
                        res.assign(reinterpret_cast<char*>(buffer),
                                   std::min<size_t>(maxLen, buflen));
                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_SIGNED_TINYINT:
                    case IGNITE_ODBC_C_TYPE_SIGNED_SHORT:
                    case IGNITE_ODBC_C_TYPE_SIGNED_LONG:
                    case IGNITE_ODBC_C_TYPE_SIGNED_BIGINT:
                    {
                        std::stringstream converter;

                        converter << GetNum<int64_t>();

                        res = converter.str();

                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_BIT:
                    case IGNITE_ODBC_C_TYPE_UNSIGNED_TINYINT:
                    case IGNITE_ODBC_C_TYPE_UNSIGNED_SHORT:
                    case IGNITE_ODBC_C_TYPE_UNSIGNED_LONG:
                    case IGNITE_ODBC_C_TYPE_UNSIGNED_BIGINT:
                    {
                        std::stringstream converter;

                        converter << GetNum<uint64_t>();

                        res = converter.str();

                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_FLOAT:
                    {
                        std::stringstream converter;

                        converter << GetNum<float>();

                        res = converter.str();

                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_DOUBLE:
                    {
                        std::stringstream converter;

                        converter << GetNum<double>();

                        res = converter.str();

                        break;
                    }

                    default:
                        break;
                }

                return res;
            }

            int8_t ApplicationDataBuffer::GetInt8() const
            {
                return GetNum<int8_t>();
            }

            int16_t ApplicationDataBuffer::GetInt16() const
            {
                return GetNum<int16_t>();
            }

            int32_t ApplicationDataBuffer::GetInt32() const
            {
                return GetNum<int32_t>();
            }

            int64_t ApplicationDataBuffer::GetInt64() const
            {
                return GetNum<int64_t>();
            }

            float ApplicationDataBuffer::GetFloat() const
            {
                return GetNum<float>();
            }

            double ApplicationDataBuffer::GetDouble() const
            {
                return GetNum<double>();
            }

            template<typename T>
            T ApplicationDataBuffer::GetNum() const
            {
                using namespace type_traits;

                T res = 0;

                switch (type)
                {
                    case IGNITE_ODBC_C_TYPE_CHAR:
                    {
                        std::string str = GetString(buflen);

                        std::stringstream converter(str);

                        converter >> res;

                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_SIGNED_TINYINT:
                    {
                        res = static_cast<T>(*reinterpret_cast<int8_t*>(buffer));
                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_BIT:
                    case IGNITE_ODBC_C_TYPE_UNSIGNED_TINYINT:
                    {
                        res = static_cast<T>(*reinterpret_cast<uint8_t*>(buffer));
                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_SIGNED_SHORT:
                    {
                        res = static_cast<T>(*reinterpret_cast<int16_t*>(buffer));
                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_UNSIGNED_SHORT:
                    {
                        res = static_cast<T>(*reinterpret_cast<uint16_t*>(buffer));
                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_SIGNED_LONG:
                    {
                        res = static_cast<T>(*reinterpret_cast<int32_t*>(buffer));
                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_UNSIGNED_LONG:
                    {
                        res = static_cast<T>(*reinterpret_cast<uint32_t*>(buffer));
                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_SIGNED_BIGINT:
                    {
                        res = static_cast<T>(*reinterpret_cast<int64_t*>(buffer));
                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_UNSIGNED_BIGINT:
                    {
                        res = static_cast<T>(*reinterpret_cast<uint64_t*>(buffer));
                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_FLOAT:
                    {
                        res = static_cast<T>(*reinterpret_cast<float*>(buffer));
                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_DOUBLE:
                    {
                        res = static_cast<T>(*reinterpret_cast<double*>(buffer));
                        break;
                    }

                    default:
                        break;
                }

                return res;
            }
        }
    }
}

