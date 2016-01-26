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
#include "ignite/odbc/app/application_data_buffer.h"
#include "ignite/odbc/utility.h"

namespace ignite
{
    namespace odbc
    {
        namespace app
        {
            ApplicationDataBuffer::ApplicationDataBuffer() :
                type(type_traits::IGNITE_ODBC_C_TYPE_UNSUPPORTED), buffer(0), buflen(0), reslen(0), offset(0)
            {
                // No-op.
            }

            ApplicationDataBuffer::ApplicationDataBuffer(type_traits::IgniteSqlType type, 
                void* buffer, SqlLen buflen, SqlLen* reslen, size_t** offset) :
                type(type), buffer(buffer), buflen(buflen), reslen(reslen), offset(offset)
            {
                // No-op.
            }

            ApplicationDataBuffer::ApplicationDataBuffer(const ApplicationDataBuffer & other) :
                type(other.type), buffer(other.buffer), buflen(other.buflen), reslen(other.reslen), offset(other.offset)
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
                offset = other.offset;

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
                        if (GetData())
                        {
                            SQL_NUMERIC_STRUCT* out =
                                reinterpret_cast<SQL_NUMERIC_STRUCT*>(GetData());

                            out->precision = 0;
                            out->scale = 0;
                            out->sign = value > 0 ? 1 : 0;

                            memset(out->val, 0, SQL_MAX_NUMERIC_LEN);

                            // TODO: implement propper conversation to numeric type.
                            int64_t intVal = static_cast<int64_t>(std::abs(value));

                            memcpy(out->val, &intVal, std::min<int>(SQL_MAX_NUMERIC_LEN, sizeof(intVal)));
                        }
                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_BINARY:
                    case IGNITE_ODBC_C_TYPE_DEFAULT:
                    {
                        if (GetData())
                        {
                            if (buflen >= sizeof(value))
                            {
                                memcpy(GetData(), &value, sizeof(value));

                                if (GetResLen())
                                    *GetResLen() = sizeof(value);
                            }
                            else
                            {
                                memcpy(GetData(), &value, static_cast<size_t>(buflen));

                                if (GetResLen())
                                    *GetResLen() = SQL_NO_TOTAL;
                            }
                        }
                        else if (GetResLen())
                        {
                            *GetResLen() = sizeof(value);
                        }
                        break;
                    }

                    default:
                    {
                        if (GetResLen())
                            *GetResLen() = SQL_NO_TOTAL;
                    }
                }
            }

            template<typename Tbuf, typename Tin>
            void ApplicationDataBuffer::PutNumToNumBuffer(Tin value)
            {
                if (GetData())
                {
                    Tbuf* out = reinterpret_cast<Tbuf*>(GetData());
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
                SqlLen charSize = static_cast<SqlLen>(sizeof(OutCharT));

                if (GetData())
                {
                    if (buflen >= charSize)
                    {
                        OutCharT* out = reinterpret_cast<OutCharT*>(GetData());

                        SqlLen outLen = (buflen / charSize) - 1;

                        SqlLen toCopy = std::min<size_t>(outLen, value.size());

                        for (SqlLen i = 0; i < toCopy; ++i)
                            out[i] = value[i];

                        out[toCopy] = 0;
                    }

                    if (GetResLen())
                    {
                        if (buflen >= static_cast<SqlLen>((value.size() + 1) * charSize))
                            *GetResLen() = static_cast<SqlLen>(value.size());
                        else
                            *GetResLen() = SQL_NO_TOTAL;
                    }
                }
                else if (GetResLen())
                    *GetResLen() = value.size();
            }

            void ApplicationDataBuffer::PutRawDataToBuffer(void *data, size_t len)
            {
                SqlLen ilen = static_cast<SqlLen>(len);

                if (GetData())
                {
                    size_t toCopy = static_cast<size_t>(std::min(buflen, ilen));

                    memcpy(GetData(), data, toCopy);

                    if (GetResLen())
                    {
                        if (buflen >= ilen)
                            *GetResLen() = ilen;
                        else
                            *GetResLen() = SQL_NO_TOTAL;
                    }
                }
                else if (GetResLen())
                    *GetResLen() = ilen;
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

            int32_t ApplicationDataBuffer::PutString(const std::string & value)
            {
                using namespace type_traits;

                int32_t used = 0;

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

                        used = static_cast<int32_t>(value.size());

                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_FLOAT:
                    case IGNITE_ODBC_C_TYPE_DOUBLE:
                    {
                        std::stringstream converter(value);

                        double numValue;

                        converter >> numValue;

                        PutNum(numValue);

                        used = static_cast<int32_t>(value.size());

                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_CHAR:
                    case IGNITE_ODBC_C_TYPE_BINARY:
                    case IGNITE_ODBC_C_TYPE_DEFAULT:
                    {
                        PutStrToStrBuffer<char>(value);

                        used = static_cast<int32_t>(GetSize()) - 1;

                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_WCHAR:
                    {
                        PutStrToStrBuffer<wchar_t>(value);

                        used = (static_cast<int32_t>(GetSize()) / 2) - 1;

                        break;
                    }

                    default:
                    {
                        if (GetResLen())
                            *GetResLen() = SQL_NO_TOTAL;
                    }
                }

                return used < 0 ? 0 : used;
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
                        SQLGUID* guid = reinterpret_cast<SQLGUID*>(GetData());

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
                        if (GetResLen())
                            *GetResLen() = SQL_NO_TOTAL;
                    }
                }
            }

            int32_t ApplicationDataBuffer::PutBinaryData(void *data, size_t len)
            {
                using namespace type_traits;

                int32_t used = 0;

                switch (type)
                {
                    case IGNITE_ODBC_C_TYPE_BINARY:
                    case IGNITE_ODBC_C_TYPE_DEFAULT:
                    {
                        PutRawDataToBuffer(data, len);

                        used = static_cast<int32_t>(GetSize());

                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_CHAR:
                    {
                        std::stringstream converter;

                        uint8_t *dataBytes = reinterpret_cast<uint8_t*>(data);

                        for (size_t i = 0; i < len; ++i)
                        {
                            converter << std::hex
                                      << std::setfill('0')
                                      << std::setw(2)
                                      << static_cast<unsigned>(dataBytes[i]);
                        }

                        PutStrToStrBuffer<char>(converter.str());

                        used = static_cast<int32_t>(GetSize()) - 1;

                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_WCHAR:
                    {
                        std::wstringstream converter;

                        uint8_t *dataBytes = reinterpret_cast<uint8_t*>(data);

                        for (size_t i = 0; i < len; ++i)
                        {
                            converter << std::hex
                                      << std::setfill<wchar_t>('0')
                                      << std::setw(2)
                                      << static_cast<unsigned>(dataBytes[i]);
                        }

                        PutStrToStrBuffer<wchar_t>(converter.str());

                        used = static_cast<int32_t>(GetSize() / 2) - 1;

                        break;
                    }

                    default:
                    {
                        if (GetResLen())
                            *GetResLen() = SQL_NO_TOTAL;
                    }
                }

                return used < 0 ? 0 : used;
            }

            void ApplicationDataBuffer::PutNull()
            {
                if (GetResLen())
                    *GetResLen() = SQL_NULL_DATA;
            }

            void ApplicationDataBuffer::PutDecimal(const Decimal& value)
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
                    case IGNITE_ODBC_C_TYPE_FLOAT:
                    case IGNITE_ODBC_C_TYPE_DOUBLE:
                    case IGNITE_ODBC_C_TYPE_CHAR:
                    case IGNITE_ODBC_C_TYPE_WCHAR:
                    {
                        PutNum<double>(static_cast<double>(value));

                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_NUMERIC:
                    {
                        if (GetData())
                        {
                            SQL_NUMERIC_STRUCT* numeric =
                                reinterpret_cast<SQL_NUMERIC_STRUCT*>(GetData());

                            numeric->sign = value.IsNegative() ? 1 : 0;
                            numeric->precision = 0;
                            numeric->scale = value.GetScale();
                            memcpy(numeric->val, value.GetMagnitude(), std::min<size_t>(SQL_MAX_NUMERIC_LEN, value.GetLength()));
                        }

                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_DEFAULT:
                    {
                        if (GetData())
                            memcpy(GetData(), &value, std::min(static_cast<size_t>(buflen), sizeof(value)));

                        if (GetResLen())
                            *GetResLen() = sizeof(value);

                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_BINARY:
                    default:
                    {
                        if (GetResLen())
                            *GetResLen() = SQL_NO_TOTAL;
                    }
                }
            }

            std::string ApplicationDataBuffer::GetString(size_t maxLen) const
            {
                using namespace type_traits;
                std::string res;

                switch (type)
                {
                    case IGNITE_ODBC_C_TYPE_CHAR:
                    {
                        res.assign(reinterpret_cast<const char*>(GetData()),
                                   std::min(maxLen, static_cast<size_t>(buflen)));
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

                    case IGNITE_ODBC_C_TYPE_NUMERIC:
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

            const void* ApplicationDataBuffer::GetData() const
            {
                return ApplyOffset(buffer);
            }

            const SqlLen* ApplicationDataBuffer::GetResLen() const
            {
                return ApplyOffset(reslen);
            }

            void* ApplicationDataBuffer::GetData() 
            {
                return ApplyOffset(buffer);
            }

            SqlLen* ApplicationDataBuffer::GetResLen()
            {
                return ApplyOffset(reslen);
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
                        std::string str = GetString(static_cast<size_t>(buflen));

                        std::stringstream converter(str);

                        // Workaround for char types which are recognised as
                        // symbolyc types and not numeric types.
                        if (sizeof(T) == 1)
                        {
                            short tmp;

                            converter >> tmp;

                            res = static_cast<T>(tmp);
                        }
                        else
                            converter >> res;

                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_SIGNED_TINYINT:
                    {
                        res = static_cast<T>(*reinterpret_cast<const signed char*>(GetData()));
                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_BIT:
                    case IGNITE_ODBC_C_TYPE_UNSIGNED_TINYINT:
                    {
                        res = static_cast<T>(*reinterpret_cast<const unsigned char*>(GetData()));
                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_SIGNED_SHORT:
                    {
                        res = static_cast<T>(*reinterpret_cast<const signed short*>(GetData()));
                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_UNSIGNED_SHORT:
                    {
                        res = static_cast<T>(*reinterpret_cast<const unsigned short*>(GetData()));
                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_SIGNED_LONG:
                    {
                        res = static_cast<T>(*reinterpret_cast<const signed long*>(GetData()));
                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_UNSIGNED_LONG:
                    {
                        res = static_cast<T>(*reinterpret_cast<const unsigned long*>(GetData()));
                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_SIGNED_BIGINT:
                    {
                        res = static_cast<T>(*reinterpret_cast<const int64_t*>(GetData()));
                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_UNSIGNED_BIGINT:
                    {
                        res = static_cast<T>(*reinterpret_cast<const uint64_t*>(GetData()));
                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_FLOAT:
                    {
                        res = static_cast<T>(*reinterpret_cast<const float*>(GetData()));
                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_DOUBLE:
                    {
                        res = static_cast<T>(*reinterpret_cast<const double*>(GetData()));
                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_NUMERIC:
                    {
                        const SQL_NUMERIC_STRUCT* numeric =
                            reinterpret_cast<const SQL_NUMERIC_STRUCT*>(GetData());

                        int64_t resInt;

                        // TODO: implement propper conversation from numeric type.
                        memcpy(&resInt, numeric->val, std::min<int>(SQL_MAX_NUMERIC_LEN, sizeof(resInt)));

                        if (numeric->sign)
                            resInt *= -1;

                        double resDouble = static_cast<double>(resInt);

                        for (SQLSCHAR scale = numeric->scale; scale > 0; --scale)
                            resDouble /= 10.0;

                        res = static_cast<T>(resDouble);

                        break;
                    }

                    default:
                        break;
                }

                return res;
            }

            template<typename T>
            T* ApplicationDataBuffer::ApplyOffset(T* ptr) const
            {
                if (!ptr || !offset || !*offset)
                    return ptr;

                return utility::GetPointerWithOffset(ptr, **offset);
            }
        }
    }
}

