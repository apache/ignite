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

#include <time.h>

#include "ignite/impl/interop/interop.h"
#include "ignite/impl/binary/binary_utils.h"

using namespace ignite::impl::interop;
using namespace ignite::impl::binary;

namespace ignite
{
    namespace impl
    {
        namespace binary
        {
            int8_t BinaryUtils::ReadInt8(InteropInputStream* stream)
            {
                return stream->ReadInt8();
            }

            void BinaryUtils::WriteInt8(InteropOutputStream* stream, int8_t val)
            {
                stream->WriteInt8(val); 
            }

            void BinaryUtils::ReadInt8Array(InteropInputStream* stream, int8_t* res, const int32_t len)
            {
                stream->ReadInt8Array(res, len);
            }

            void BinaryUtils::WriteInt8Array(InteropOutputStream* stream, const int8_t* val, const int32_t len)
            {
                stream->WriteInt8Array(val, len);
            }

            bool BinaryUtils::ReadBool(InteropInputStream* stream)
            {
                return stream->ReadBool();
            }

            void BinaryUtils::WriteBool(InteropOutputStream* stream, bool val)
            {
                stream->WriteBool(val);
            }

            void BinaryUtils::ReadBoolArray(InteropInputStream* stream, bool* res, const int32_t len)
            {
                stream->ReadBoolArray(res, len);
            }

            void BinaryUtils::WriteBoolArray(InteropOutputStream* stream, const bool* val, const int32_t len)
            {
                stream->WriteBoolArray(val, len);
            }

            int16_t BinaryUtils::ReadInt16(InteropInputStream* stream)
            {
                return stream->ReadInt16();
            }

            void BinaryUtils::WriteInt16(InteropOutputStream* stream, int16_t val)
            {
                stream->WriteInt16(val);
            }

            void BinaryUtils::ReadInt16Array(InteropInputStream* stream, int16_t* res, const int32_t len)
            {
                stream->ReadInt16Array(res, len);
            }
            
            void BinaryUtils::WriteInt16Array(InteropOutputStream* stream, const int16_t* val, const int32_t len)
            {
                stream->WriteInt16Array(val, len);
            }

            uint16_t BinaryUtils::ReadUInt16(InteropInputStream* stream)
            {
                return stream->ReadUInt16();
            }

            void BinaryUtils::WriteUInt16(InteropOutputStream* stream, uint16_t val)
            {
                stream->WriteUInt16(val);
            }

            void BinaryUtils::ReadUInt16Array(InteropInputStream* stream, uint16_t* res, const int32_t len)
            {
                stream->ReadUInt16Array(res, len);
            }

            void BinaryUtils::WriteUInt16Array(InteropOutputStream* stream, const uint16_t* val, const int32_t len)
            {
                stream->WriteUInt16Array(val, len);
            }

            int32_t BinaryUtils::ReadInt32(InteropInputStream* stream)
            {
                return stream->ReadInt32();
            }

            void BinaryUtils::WriteInt32(InteropOutputStream* stream, int32_t val)
            {
                stream->WriteInt32(val);
            }

            void BinaryUtils::ReadInt32Array(InteropInputStream* stream, int32_t* res, const int32_t len)
            {
                stream->ReadInt32Array(res, len);
            }

            void BinaryUtils::WriteInt32Array(InteropOutputStream* stream, const int32_t* val, const int32_t len)
            {
                stream->WriteInt32Array(val, len);
            }

            int64_t BinaryUtils::ReadInt64(InteropInputStream* stream)
            {
                return stream->ReadInt64();
            }

            void BinaryUtils::WriteInt64(InteropOutputStream* stream, int64_t val)
            {
                stream->WriteInt64(val);
            }

            void BinaryUtils::ReadInt64Array(InteropInputStream* stream, int64_t* res, const int32_t len)
            {
                stream->ReadInt64Array(res, len);
            }

            void BinaryUtils::WriteInt64Array(InteropOutputStream* stream, const int64_t* val, const int32_t len)
            {
                stream->WriteInt64Array(val, len);
            }

            float BinaryUtils::ReadFloat(InteropInputStream* stream)
            {
                return stream->ReadFloat();
            }

            void BinaryUtils::WriteFloat(InteropOutputStream* stream, float val)
            {
                stream->WriteFloat(val);
            }

            void BinaryUtils::ReadFloatArray(InteropInputStream* stream, float* res, const int32_t len)
            {
                stream->ReadFloatArray(res, len);
            }

            void BinaryUtils::WriteFloatArray(InteropOutputStream* stream, const float* val, const int32_t len)
            {
                stream->WriteFloatArray(val, len);
            }

            double BinaryUtils::ReadDouble(InteropInputStream* stream)
            {
                return stream->ReadDouble();
            }

            void BinaryUtils::WriteDouble(InteropOutputStream* stream, double val)
            {
                stream->WriteDouble(val);
            }

            void BinaryUtils::ReadDoubleArray(InteropInputStream* stream, double* res, const int32_t len)
            {
                stream->ReadDoubleArray(res, len);
            }

            void BinaryUtils::WriteDoubleArray(InteropOutputStream* stream, const double* val, const int32_t len)
            {
                stream->WriteDoubleArray(val, len);
            }

            Guid BinaryUtils::ReadGuid(interop::InteropInputStream* stream)
            {
                int64_t most = stream->ReadInt64();
                int64_t least = stream->ReadInt64();

                return Guid(most, least);
            }

            void BinaryUtils::WriteGuid(interop::InteropOutputStream* stream, const Guid val)
            {
                stream->WriteInt64(val.GetMostSignificantBits());
                stream->WriteInt64(val.GetLeastSignificantBits());
            }

            Date BinaryUtils::ReadDate(interop::InteropInputStream * stream)
            {
                int64_t milliseconds = stream->ReadInt64();

                return Date(milliseconds);
            }

            void BinaryUtils::WriteDate(interop::InteropOutputStream* stream, const Date val)
            {
                stream->WriteInt64(val.GetMilliseconds());
            }

            Timestamp BinaryUtils::ReadTimestamp(interop::InteropInputStream* stream)
            {
                int64_t milliseconds = stream->ReadInt64();
                int32_t nanoseconds = stream->ReadInt32();

                return Timestamp(milliseconds / 1000, nanoseconds);
            }

            void BinaryUtils::WriteTimestamp(interop::InteropOutputStream* stream, const Timestamp val)
            {
                stream->WriteInt64(val.GetSeconds() * 1000);
                stream->WriteInt32(val.GetSecondFraction());
            }

            void BinaryUtils::WriteString(interop::InteropOutputStream* stream, const char* val, const int32_t len)
            {
                stream->WriteInt32(len);
                stream->WriteInt8Array(reinterpret_cast<const int8_t*>(val), len);
            }

            Date BinaryUtils::MakeDateGmt(int year, int month, int day, int hour,
                int min, int sec)
            {
                tm date = { 0 };

                date.tm_year = year - 1900;
                date.tm_mon = month - 1;
                date.tm_mday = day;
                date.tm_hour = hour;
                date.tm_min = min;
                date.tm_sec = sec;

                return CTmToDate(date);
            }

            Date BinaryUtils::MakeDateLocal(int year, int month, int day, int hour,
                int min, int sec)
            {
                tm date = { 0 };

                date.tm_year = year - 1900;
                date.tm_mon = month - 1;
                date.tm_mday = day;
                date.tm_hour = hour;
                date.tm_min = min;
                date.tm_sec = sec;

                time_t localTime = common::IgniteTimeLocal(date);

                return CTimeToDate(localTime);
            }

            Timestamp BinaryUtils::MakeTimestampGmt(int year, int month, int day,
                int hour, int min, int sec, long ns)
            {
                tm date = { 0 };

                date.tm_year = year - 1900;
                date.tm_mon = month - 1;
                date.tm_mday = day;
                date.tm_hour = hour;
                date.tm_min = min;
                date.tm_sec = sec;

                return CTmToTimestamp(date, ns);
            }

            Timestamp BinaryUtils::MakeTimestampLocal(int year, int month, int day,
                int hour, int min, int sec, long ns)
            {
                tm date = { 0 };

                date.tm_year = year - 1900;
                date.tm_mon = month - 1;
                date.tm_mday = day;
                date.tm_hour = hour;
                date.tm_min = min;
                date.tm_sec = sec;

                time_t localTime = common::IgniteTimeLocal(date);

                return CTimeToTimestamp(localTime, ns);
            }
        }
    }
}
