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

#include "ignite/common/bits.h"

#include "ignite/impl/binary/binary_utils.h"

#include "ignite/odbc/system/odbc_constants.h"
#include "ignite/odbc/app/application_data_buffer.h"
#include "ignite/odbc/utility.h"

namespace ignite
{
    namespace odbc
    {
        namespace app
        {
            using ignite::impl::binary::BinaryUtils;

            ApplicationDataBuffer::ApplicationDataBuffer() :
                type(type_traits::IGNITE_ODBC_C_TYPE_UNSUPPORTED),
                buffer(0),
                buflen(0),
                reslen(0),
                offset(0)
            {
                // No-op.
            }

            ApplicationDataBuffer::ApplicationDataBuffer(type_traits::IgniteSqlType type,
                void* buffer, SqlLen buflen, SqlLen* reslen, int** offset) :
                type(type),
                buffer(buffer),
                buflen(buflen),
                reslen(reslen),
                offset(offset)
            {
                // No-op.
            }

            ApplicationDataBuffer::ApplicationDataBuffer(const ApplicationDataBuffer & other) :
                type(other.type),
                buffer(other.buffer),
                buflen(other.buflen),
                reslen(other.reslen),
                offset(other.offset)
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

                SqlLen* resLenPtr = GetResLen();
                void* dataPtr = GetData();

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
                        if (dataPtr)
                        {
                            SQL_NUMERIC_STRUCT* out =
                                reinterpret_cast<SQL_NUMERIC_STRUCT*>(dataPtr);

                            uint64_t uval = static_cast<uint64_t>(value < 0 ? -value : value);

                            out->precision = common::bits::DigitLength(uval);
                            out->scale = 0;
                            out->sign = value < 0 ? 0 : 1;

                            memset(out->val, 0, SQL_MAX_NUMERIC_LEN);

                            memcpy(out->val, &uval, std::min<int>(SQL_MAX_NUMERIC_LEN, sizeof(uval)));
                        }

                        if (resLenPtr)
                            *resLenPtr = static_cast<SqlLen>(sizeof(SQL_NUMERIC_STRUCT));

                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_BINARY:
                    case IGNITE_ODBC_C_TYPE_DEFAULT:
                    {
                        if (dataPtr)
                        {
                            if (buflen >= sizeof(value))
                            {
                                memcpy(dataPtr, &value, sizeof(value));

                                if (resLenPtr)
                                    *resLenPtr = sizeof(value);
                            }
                            else
                            {
                                memcpy(dataPtr, &value, static_cast<size_t>(buflen));

                                if (resLenPtr)
                                    *resLenPtr = SQL_NO_TOTAL;
                            }
                        }
                        else if (resLenPtr)
                            *resLenPtr = sizeof(value);

                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_TDATE:
                    {
                        PutDate(Date(static_cast<int64_t>(value)));

                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_TTIMESTAMP:
                    {
                        PutTimestamp(Timestamp(static_cast<int64_t>(value)));

                        break;
                    }

                    default:
                    {
                        if (resLenPtr)
                            *resLenPtr = SQL_NO_TOTAL;
                    }
                }
            }

            template<typename Tbuf, typename Tin>
            void ApplicationDataBuffer::PutNumToNumBuffer(Tin value)
            {
                void* dataPtr = GetData();
                SqlLen* resLenPtr = GetResLen();

                if (dataPtr)
                {
                    Tbuf* out = reinterpret_cast<Tbuf*>(dataPtr);
                    *out = static_cast<Tbuf>(value);
                }

                if (resLenPtr)
                    *resLenPtr = static_cast<SqlLen>(sizeof(Tbuf));
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

                SqlLen* resLenPtr = GetResLen();
                void* dataPtr = GetData();

                if (dataPtr)
                {
                    if (buflen >= charSize)
                    {
                        OutCharT* out = reinterpret_cast<OutCharT*>(dataPtr);

                        SqlLen outLen = (buflen / charSize) - 1;

                        SqlLen toCopy = std::min<size_t>(outLen, value.size());

                        for (SqlLen i = 0; i < toCopy; ++i)
                            out[i] = value[i];

                        out[toCopy] = 0;
                    }

                    if (resLenPtr)
                    {
                        if (buflen >= static_cast<SqlLen>((value.size() + 1) * charSize))
                            *resLenPtr = static_cast<SqlLen>(value.size());
                        else
                            *resLenPtr = SQL_NO_TOTAL;
                    }
                }
                else if (resLenPtr)
                    *resLenPtr = value.size();
            }

            void ApplicationDataBuffer::PutRawDataToBuffer(void *data, size_t len)
            {
                SqlLen ilen = static_cast<SqlLen>(len);

                SqlLen* resLenPtr = GetResLen();
                void* dataPtr = GetData();

                if (dataPtr)
                {
                    size_t toCopy = static_cast<size_t>(std::min(buflen, ilen));

                    memcpy(dataPtr, data, toCopy);

                    if (resLenPtr)
                    {
                        if (buflen >= ilen)
                            *resLenPtr = ilen;
                        else
                            *resLenPtr = SQL_NO_TOTAL;
                    }
                }
                else if (resLenPtr)
                    *resLenPtr = ilen;
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

            int32_t ApplicationDataBuffer::PutString(const std::string& value)
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
                        std::stringstream converter;

                        converter << value;

                        int64_t numValue;

                        converter >> numValue;

                        PutNum(numValue);

                        used = static_cast<int32_t>(value.size());

                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_FLOAT:
                    case IGNITE_ODBC_C_TYPE_DOUBLE:
                    {
                        std::stringstream converter;

                        converter << value;

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
                        SqlLen* resLenPtr = GetResLen();

                        if (resLenPtr)
                            *resLenPtr = SQL_NO_TOTAL;
                    }
                }

                return used < 0 ? 0 : used;
            }

            void ApplicationDataBuffer::PutGuid(const Guid & value)
            {
                using namespace type_traits;

                SqlLen* resLenPtr = GetResLen();

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

                        if (resLenPtr)
                            *resLenPtr = static_cast<SqlLen>(sizeof(SQLGUID));

                        break;
                    }

                    default:
                    {
                        if (resLenPtr)
                            *resLenPtr = SQL_NO_TOTAL;
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
                        SqlLen* resLenPtr = GetResLen();

                        if (resLenPtr)
                            *resLenPtr = SQL_NO_TOTAL;
                    }
                }

                return used < 0 ? 0 : used;
            }

            void ApplicationDataBuffer::PutNull()
            {
                SqlLen* resLenPtr = GetResLen();

                if (resLenPtr)
                    *resLenPtr = SQL_NULL_DATA;
            }

            void ApplicationDataBuffer::PutDecimal(const common::Decimal& value)
            {
                using namespace type_traits;

                SqlLen* resLenPtr = GetResLen();

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
                    {
                        PutNum<int64_t>(value.ToInt64());

                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_FLOAT:
                    case IGNITE_ODBC_C_TYPE_DOUBLE:
                    {
                        PutNum<double>(value.ToDouble());

                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_CHAR:
                    case IGNITE_ODBC_C_TYPE_WCHAR:
                    {
                        std::stringstream converter;

                        converter << value;

                        PutString(converter.str());

                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_NUMERIC:
                    {
                        SQL_NUMERIC_STRUCT* numeric =
                            reinterpret_cast<SQL_NUMERIC_STRUCT*>(GetData());

                        common::Decimal zeroScaled;
                        value.SetScale(0, zeroScaled);

                        common::FixedSizeArray<int8_t> bytesBuffer;

                        const common::BigInteger& unscaled = zeroScaled.GetUnscaledValue();

                        unscaled.MagnitudeToBytes(bytesBuffer);

                        for (int32_t i = 0; i < SQL_MAX_NUMERIC_LEN; ++i)
                        {
                            int32_t bufIdx = bytesBuffer.GetSize() - 1 - i;
                            if (bufIdx >= 0)
                                numeric->val[i] = bytesBuffer[bufIdx];
                            else
                                numeric->val[i] = 0;
                        }

                        numeric->scale = 0;
                        numeric->sign = unscaled.GetSign() < 0 ? 0 : 1;
                        numeric->precision = unscaled.GetPrecision();

                        if (resLenPtr)
                            *resLenPtr = static_cast<SqlLen>(sizeof(SQL_NUMERIC_STRUCT));

                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_DEFAULT:
                    case IGNITE_ODBC_C_TYPE_BINARY:
                    default:
                    {
                        if (resLenPtr)
                            *resLenPtr = SQL_NO_TOTAL;
                    }
                }
            }

            void ApplicationDataBuffer::PutDate(const Date& value)
            {
                using namespace type_traits;

                tm tmTime;

                common::DateToCTm(value, tmTime);

                SqlLen* resLenPtr = GetResLen();
                void* dataPtr = GetData();

                switch (type)
                {
                    case IGNITE_ODBC_C_TYPE_CHAR:
                    {
                        char* buffer = reinterpret_cast<char*>(dataPtr);

                        if (buffer)
                        {
                            strftime(buffer, GetSize(), "%Y-%m-%d", &tmTime);

                            if (resLenPtr)
                                *resLenPtr = strlen(buffer);
                        }
                        else if (resLenPtr)
                            *resLenPtr = sizeof("HHHH-MM-DD") - 1;

                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_WCHAR:
                    {
                        SQLWCHAR* buffer = reinterpret_cast<SQLWCHAR*>(dataPtr);

                        if (buffer)
                        {
                            std::string tmp(GetSize(), 0);

                            strftime(&tmp[0], GetSize(), "%Y-%m-%d", &tmTime);

                            SqlLen toCopy = std::min(static_cast<SqlLen>(strlen(tmp.c_str()) + 1), GetSize());

                            for (SqlLen i = 0; i < toCopy; ++i)
                                buffer[i] = tmp[i];

                            buffer[toCopy] = 0;

                            if (resLenPtr)
                                *resLenPtr = toCopy;
                        }
                        else if (resLenPtr)
                            *resLenPtr = sizeof("HHHH-MM-DD") - 1;

                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_TDATE:
                    {
                        SQL_DATE_STRUCT* buffer = reinterpret_cast<SQL_DATE_STRUCT*>(dataPtr);

                        buffer->year = tmTime.tm_year + 1900;
                        buffer->month = tmTime.tm_mon + 1;
                        buffer->day = tmTime.tm_mday;

                        if (resLenPtr)
                            *resLenPtr = static_cast<SqlLen>(sizeof(SQL_DATE_STRUCT));

                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_TTIME:
                    {
                        SQL_TIME_STRUCT* buffer = reinterpret_cast<SQL_TIME_STRUCT*>(dataPtr);

                        buffer->hour = tmTime.tm_hour;
                        buffer->minute = tmTime.tm_min;
                        buffer->second = tmTime.tm_sec;

                        if (resLenPtr)
                            *resLenPtr = static_cast<SqlLen>(sizeof(SQL_TIME_STRUCT));

                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_TTIMESTAMP:
                    {
                        SQL_TIMESTAMP_STRUCT* buffer = reinterpret_cast<SQL_TIMESTAMP_STRUCT*>(dataPtr);

                        buffer->year = tmTime.tm_year + 1900;
                        buffer->month = tmTime.tm_mon + 1;
                        buffer->day = tmTime.tm_mday;
                        buffer->hour = tmTime.tm_hour;
                        buffer->minute = tmTime.tm_min;
                        buffer->second = tmTime.tm_sec;
                        buffer->fraction = 0;

                        if (resLenPtr)
                            *resLenPtr = static_cast<SqlLen>(sizeof(SQL_TIMESTAMP_STRUCT));

                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_BINARY:
                    case IGNITE_ODBC_C_TYPE_DEFAULT:
                    {
                        if (dataPtr)
                            memcpy(dataPtr, &value, std::min(static_cast<size_t>(buflen), sizeof(value)));

                        if (resLenPtr)
                            *resLenPtr = sizeof(value);

                        break;
                    }

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
                    case IGNITE_ODBC_C_TYPE_NUMERIC:
                    default:
                    {
                        if (resLenPtr)
                            *resLenPtr = SQL_NO_TOTAL;
                    }
                }
            }

            void ApplicationDataBuffer::PutTimestamp(const Timestamp& value)
            {
                using namespace type_traits;

                tm tmTime;

                common::TimestampToCTm(value, tmTime);

                SqlLen* resLenPtr = GetResLen();
                void* dataPtr = GetData();

                switch (type)
                {
                    case IGNITE_ODBC_C_TYPE_CHAR:
                    {
                        char* buffer = reinterpret_cast<char*>(dataPtr);

                        if (buffer)
                        {
                            strftime(buffer, GetSize(), "%Y-%m-%d %H:%M:%S", &tmTime);

                            if (resLenPtr)
                                *resLenPtr = strlen(buffer);
                        }
                        else if (resLenPtr)
                            *resLenPtr = sizeof("HHHH-MM-DD HH:MM:SS") - 1;

                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_WCHAR:
                    {
                        SQLWCHAR* buffer = reinterpret_cast<SQLWCHAR*>(dataPtr);

                        if (buffer)
                        {
                            std::string tmp(GetSize(), 0);

                            strftime(&tmp[0], GetSize(), "%Y-%m-%d %H:%M:%S", &tmTime);

                            SqlLen toCopy = std::min(static_cast<SqlLen>(strlen(tmp.c_str()) + 1), GetSize());

                            for (SqlLen i = 0; i < toCopy; ++i)
                                buffer[i] = tmp[i];

                            buffer[toCopy] = 0;

                            if (resLenPtr)
                                *resLenPtr = toCopy;
                        }
                        else if (resLenPtr)
                            *resLenPtr = sizeof("HHHH-MM-DD HH:MM:SS") - 1;

                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_TDATE:
                    {
                        SQL_DATE_STRUCT* buffer = reinterpret_cast<SQL_DATE_STRUCT*>(dataPtr);

                        buffer->year = tmTime.tm_year + 1900;
                        buffer->month = tmTime.tm_mon + 1;
                        buffer->day = tmTime.tm_mday;

                        if (resLenPtr)
                            *resLenPtr = static_cast<SqlLen>(sizeof(SQL_DATE_STRUCT));

                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_TTIME:
                    {
                        SQL_TIME_STRUCT* buffer = reinterpret_cast<SQL_TIME_STRUCT*>(dataPtr);

                        buffer->hour = tmTime.tm_hour;
                        buffer->minute = tmTime.tm_min;
                        buffer->second = tmTime.tm_sec;

                        if (resLenPtr)
                            *resLenPtr = static_cast<SqlLen>(sizeof(SQL_TIME_STRUCT));

                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_TTIMESTAMP:
                    {
                        SQL_TIMESTAMP_STRUCT* buffer = reinterpret_cast<SQL_TIMESTAMP_STRUCT*>(dataPtr);

                        buffer->year = tmTime.tm_year + 1900;
                        buffer->month = tmTime.tm_mon + 1;
                        buffer->day = tmTime.tm_mday;
                        buffer->hour = tmTime.tm_hour;
                        buffer->minute = tmTime.tm_min;
                        buffer->second = tmTime.tm_sec;
                        buffer->fraction = value.GetSecondFraction();

                        if (resLenPtr)
                            *resLenPtr = static_cast<SqlLen>(sizeof(SQL_TIMESTAMP_STRUCT));

                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_BINARY:
                    case IGNITE_ODBC_C_TYPE_DEFAULT:
                    {
                        if (dataPtr)
                            memcpy(dataPtr, &value, std::min(static_cast<size_t>(buflen), sizeof(value)));

                        if (resLenPtr)
                            *resLenPtr = sizeof(value);

                        break;
                    }

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
                    case IGNITE_ODBC_C_TYPE_NUMERIC:
                    default:
                    {
                        if (resLenPtr)
                            *resLenPtr = SQL_NO_TOTAL;
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
                        size_t paramLen = GetInputSize();

                        if (!paramLen)
                            break;

                        res = utility::SqlStringToString(
                            reinterpret_cast<const unsigned char*>(GetData()), static_cast<int32_t>(paramLen));

                        if (res.size() > maxLen)
                            res.resize(maxLen);

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

            Guid ApplicationDataBuffer::GetGuid() const
            {
                using namespace type_traits;

                Guid res;

                switch (type)
                {
                    case IGNITE_ODBC_C_TYPE_CHAR:
                    {
                        SqlLen paramLen = GetInputSize();

                        if (!paramLen)
                            break;

                        std::string str = utility::SqlStringToString(
                            reinterpret_cast<const unsigned char*>(GetData()), static_cast<int32_t>(paramLen));

                        std::stringstream converter;

                        converter << str;

                        converter >> res;

                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_GUID:
                    {
                        const SQLGUID* guid = reinterpret_cast<const SQLGUID*>(GetData());

                        uint64_t msb = static_cast<uint64_t>(guid->Data1) << 32 |
                                       static_cast<uint64_t>(guid->Data2) << 16 |
                                       static_cast<uint64_t>(guid->Data3);

                        uint64_t lsb = 0;

                        for (size_t i = 0; i < sizeof(guid->Data4); ++i)
                            lsb = guid->Data4[i] << (sizeof(guid->Data4) - i - 1) * 8;

                        res = Guid(msb, lsb);

                        break;
                    }

                    default:
                        break;
                }

                return res;
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

                T res = T();

                switch (type)
                {
                    case IGNITE_ODBC_C_TYPE_CHAR:
                    {
                        SqlLen paramLen = GetInputSize();

                        if (!paramLen)
                            break;

                        std::string str = GetString(paramLen);

                        std::stringstream converter;

                        converter << str;

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

                        common::Decimal dec(reinterpret_cast<const int8_t*>(numeric->val),
                            SQL_MAX_NUMERIC_LEN, numeric->scale, numeric->sign ? 1 : -1, false);

                        res = static_cast<T>(dec.ToInt64());

                        break;
                    }

                    default:
                        break;
                }

                return res;
            }

            Date ApplicationDataBuffer::GetDate() const
            {
                using namespace type_traits;

                tm tmTime = { 0 };

                switch (type)
                {
                    case IGNITE_ODBC_C_TYPE_TDATE:
                    {
                        const SQL_DATE_STRUCT* buffer = reinterpret_cast<const SQL_DATE_STRUCT*>(GetData());

                        tmTime.tm_year = buffer->year - 1900;
                        tmTime.tm_mon = buffer->month - 1;
                        tmTime.tm_mday = buffer->day;

                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_TTIME:
                    {
                        const SQL_TIME_STRUCT* buffer = reinterpret_cast<const SQL_TIME_STRUCT*>(GetData());

                        tmTime.tm_year = 70;
                        tmTime.tm_mday = 1;
                        tmTime.tm_hour = buffer->hour;
                        tmTime.tm_min = buffer->minute;
                        tmTime.tm_sec = buffer->second;

                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_TTIMESTAMP:
                    {
                        const SQL_TIMESTAMP_STRUCT* buffer = reinterpret_cast<const SQL_TIMESTAMP_STRUCT*>(GetData());

                        tmTime.tm_year = buffer->year - 1900;
                        tmTime.tm_mon = buffer->month - 1;
                        tmTime.tm_mday = buffer->day;
                        tmTime.tm_hour = buffer->hour;
                        tmTime.tm_min = buffer->minute;
                        tmTime.tm_sec = buffer->second;

                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_CHAR:
                    {
                        SqlLen paramLen = GetInputSize();

                        if (!paramLen)
                            break;

                        std::string str = utility::SqlStringToString(
                            reinterpret_cast<const unsigned char*>(GetData()), static_cast<int32_t>(paramLen));

                        sscanf(str.c_str(), "%d-%d-%d %d:%d:%d", &tmTime.tm_year, &tmTime.tm_mon,
                            &tmTime.tm_mday, &tmTime.tm_hour, &tmTime.tm_min, &tmTime.tm_sec);

                        tmTime.tm_year = tmTime.tm_year - 1900;
                        tmTime.tm_mon = tmTime.tm_mon - 1;

                        break;
                    }

                    default:
                        break;
                }

                return common::CTmToDate(tmTime);
            }

            Timestamp ApplicationDataBuffer::GetTimestamp() const
            {
                using namespace type_traits;

                tm tmTime = { 0 };

                int32_t nanos = 0;

                switch (type)
                {
                    case IGNITE_ODBC_C_TYPE_TDATE:
                    {
                        const SQL_DATE_STRUCT* buffer = reinterpret_cast<const SQL_DATE_STRUCT*>(GetData());

                        tmTime.tm_year = buffer->year - 1900;
                        tmTime.tm_mon = buffer->month - 1;
                        tmTime.tm_mday = buffer->day;

                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_TTIME:
                    {
                        const SQL_TIME_STRUCT* buffer = reinterpret_cast<const SQL_TIME_STRUCT*>(GetData());

                        tmTime.tm_year = 70;
                        tmTime.tm_mday = 1;
                        tmTime.tm_hour = buffer->hour;
                        tmTime.tm_min = buffer->minute;
                        tmTime.tm_sec = buffer->second;

                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_TTIMESTAMP:
                    {
                        const SQL_TIMESTAMP_STRUCT* buffer = reinterpret_cast<const SQL_TIMESTAMP_STRUCT*>(GetData());

                        tmTime.tm_year = buffer->year - 1900;
                        tmTime.tm_mon = buffer->month - 1;
                        tmTime.tm_mday = buffer->day;
                        tmTime.tm_hour = buffer->hour;
                        tmTime.tm_min = buffer->minute;
                        tmTime.tm_sec = buffer->second;

                        nanos = buffer->fraction;

                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_CHAR:
                    {
                        SqlLen paramLen = GetInputSize();

                        if (!paramLen)
                            break;

                        std::string str = utility::SqlStringToString(
                            reinterpret_cast<const unsigned char*>(GetData()), static_cast<int32_t>(paramLen));

                        sscanf(str.c_str(), "%d-%d-%d %d:%d:%d", &tmTime.tm_year, &tmTime.tm_mon,
                            &tmTime.tm_mday, &tmTime.tm_hour, &tmTime.tm_min, &tmTime.tm_sec);

                        tmTime.tm_year = tmTime.tm_year - 1900;
                        tmTime.tm_mon = tmTime.tm_mon - 1;

                        break;
                    }

                    default:
                        break;
                }

                return common::CTmToTimestamp(tmTime, nanos);
            }

            void ApplicationDataBuffer::GetDecimal(common::Decimal& val) const
            {
                using namespace type_traits;

                switch (type)
                {
                    case IGNITE_ODBC_C_TYPE_CHAR:
                    {
                        SqlLen paramLen = GetInputSize();

                        if (!paramLen)
                            break;

                        std::string str = GetString(paramLen);

                        std::stringstream converter;

                        converter << str;

                        converter >> val;

                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_SIGNED_TINYINT:
                    case IGNITE_ODBC_C_TYPE_BIT:
                    case IGNITE_ODBC_C_TYPE_SIGNED_SHORT:
                    case IGNITE_ODBC_C_TYPE_SIGNED_LONG:
                    case IGNITE_ODBC_C_TYPE_SIGNED_BIGINT:
                    {
                        val.AssignInt64(GetNum<int64_t>());

                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_UNSIGNED_TINYINT:
                    case IGNITE_ODBC_C_TYPE_UNSIGNED_SHORT:
                    case IGNITE_ODBC_C_TYPE_UNSIGNED_LONG:
                    case IGNITE_ODBC_C_TYPE_UNSIGNED_BIGINT:
                    {
                        val.AssignUint64(GetNum<uint64_t>());

                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_FLOAT:
                    case IGNITE_ODBC_C_TYPE_DOUBLE:
                    {
                        val.AssignDouble(GetNum<double>());

                        break;
                    }

                    case IGNITE_ODBC_C_TYPE_NUMERIC:
                    {
                        const SQL_NUMERIC_STRUCT* numeric =
                            reinterpret_cast<const SQL_NUMERIC_STRUCT*>(GetData());

                        common::Decimal dec(reinterpret_cast<const int8_t*>(numeric->val),
                            SQL_MAX_NUMERIC_LEN, numeric->scale, numeric->sign ? 1 : -1, false);

                        val.Swap(dec);

                        break;
                    }

                    default:
                    {
                        val.AssignInt64(0);

                        break;
                    }
                }
            }

            template<typename T>
            T* ApplicationDataBuffer::ApplyOffset(T* ptr) const
            {
                if (!ptr || !offset || !*offset)
                    return ptr;

                return utility::GetPointerWithOffset(ptr, **offset);
            }

            bool ApplicationDataBuffer::IsDataAtExec() const
            {
                const SqlLen* resLenPtr = GetResLen();

                if (!resLenPtr)
                    return false;

                int32_t ilen = static_cast<int32_t>(*resLenPtr);

                return ilen <= SQL_LEN_DATA_AT_EXEC_OFFSET || ilen == SQL_DATA_AT_EXEC;
            }

            SqlLen ApplicationDataBuffer::GetDataAtExecSize() const
            {
                using namespace type_traits;

                switch (type)
                {
                    case IGNITE_ODBC_C_TYPE_WCHAR:
                    case IGNITE_ODBC_C_TYPE_CHAR:
                    case IGNITE_ODBC_C_TYPE_BINARY:
                    {
                        const SqlLen* resLenPtr = GetResLen();

                        if (!resLenPtr)
                            return 0;

                        int32_t ilen = static_cast<int32_t>(*resLenPtr);

                        if (ilen <= SQL_LEN_DATA_AT_EXEC_OFFSET)
                            ilen = SQL_LEN_DATA_AT_EXEC(ilen);
                        else
                            ilen = 0;

                        if (type == IGNITE_ODBC_C_TYPE_WCHAR)
                            ilen *= 2;

                        return ilen;
                    }

                    case IGNITE_ODBC_C_TYPE_SIGNED_SHORT:
                    case IGNITE_ODBC_C_TYPE_UNSIGNED_SHORT:
                        return static_cast<SqlLen>(sizeof(short));

                    case IGNITE_ODBC_C_TYPE_SIGNED_LONG:
                    case IGNITE_ODBC_C_TYPE_UNSIGNED_LONG:
                        return static_cast<SqlLen>(sizeof(long));

                    case IGNITE_ODBC_C_TYPE_FLOAT:
                        return static_cast<SqlLen>(sizeof(float));

                    case IGNITE_ODBC_C_TYPE_DOUBLE:
                        return static_cast<SqlLen>(sizeof(double));

                    case IGNITE_ODBC_C_TYPE_BIT:
                    case IGNITE_ODBC_C_TYPE_SIGNED_TINYINT:
                    case IGNITE_ODBC_C_TYPE_UNSIGNED_TINYINT:
                        return static_cast<SqlLen>(sizeof(char));

                    case IGNITE_ODBC_C_TYPE_SIGNED_BIGINT:
                    case IGNITE_ODBC_C_TYPE_UNSIGNED_BIGINT:
                        return static_cast<SqlLen>(sizeof(SQLBIGINT));

                    case IGNITE_ODBC_C_TYPE_TDATE:
                        return static_cast<SqlLen>(sizeof(SQL_DATE_STRUCT));

                    case IGNITE_ODBC_C_TYPE_TTIME:
                        return static_cast<SqlLen>(sizeof(SQL_TIME_STRUCT));

                    case IGNITE_ODBC_C_TYPE_TTIMESTAMP:
                        return static_cast<SqlLen>(sizeof(SQL_TIMESTAMP_STRUCT));

                    case IGNITE_ODBC_C_TYPE_NUMERIC:
                        return static_cast<SqlLen>(sizeof(SQL_NUMERIC_STRUCT));

                    case IGNITE_ODBC_C_TYPE_GUID:
                        return static_cast<SqlLen>(sizeof(SQLGUID));

                    case IGNITE_ODBC_C_TYPE_DEFAULT:
                    case IGNITE_ODBC_C_TYPE_UNSUPPORTED:
                    default:
                        break;
                }

                return 0;
            }

            SqlLen ApplicationDataBuffer::GetInputSize() const
            {
                if (!IsDataAtExec())
                {
                    const SqlLen *len = GetResLen();

                    return len ? *len : SQL_DEFAULT_PARAM;
                }

                return GetDataAtExecSize();
            }
        }
    }
}

