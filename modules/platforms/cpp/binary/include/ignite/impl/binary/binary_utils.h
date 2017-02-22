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

#ifndef _IGNITE_IMPL_BINARY_BINARY_UTILS
#define _IGNITE_IMPL_BINARY_BINARY_UTILS

#include <stdint.h>

#include "ignite/common/utils.h"

#include "ignite/guid.h"
#include "ignite/date.h"
#include "ignite/timestamp.h"

#include "ignite/binary/binary_type.h"

namespace ignite
{
    namespace impl
    {
        namespace interop
        {
            class InteropInputStream;
            class InteropOutputStream;
            class InteropMemory;
        }

        namespace binary
        {
            /**
             * Binary uilts.
             */
            class IGNITE_IMPORT_EXPORT BinaryUtils
            {
            public:
                /**
                 * Get data hash code.
                 *
                 * @param data Data pointer.
                 * @param size Data size in bytes.
                 * @return Hash code.
                 */
                static int32_t GetDataHashCode(const void* data, size_t size);

                /**
                 * Utility method to read signed 8-bit integer from stream.
                 *
                 * @param stream Stream.
                 * @return Value.
                 */
                static int8_t ReadInt8(interop::InteropInputStream* stream);

                /**
                 * Utility method to read signed 8-bit integer from memory.
                 * @throw IgniteError if there is not enough memory.
                 *
                 * @param mem Memory.
                 * @param pos Position in memory.
                 * @return Value.
                 */
                static int8_t ReadInt8(interop::InteropMemory& mem, int32_t pos);

                /**
                 * Utility method to read signed 8-bit integer from memory.
                 * @warning Does not check if there is enough data in memory to read.
                 *
                 * @param mem Memory.
                 * @param pos Position in memory.
                 * @return Value.
                 */
                static int8_t UnsafeReadInt8(interop::InteropMemory& mem, int32_t pos);

                /**
                 * Utility method to write signed 8-bit integer to stream.
                 *
                 * @param stream Stream.
                 * @param val Value.
                 */
                static void WriteInt8(interop::InteropOutputStream* stream, int8_t val);

                /**
                 * Utility method to read signed 8-bit integer array from stream.
                 *
                 * @param stream Stream.
                 * @param res Target array.
                 * @param len Array length.                 
                 */
                static void ReadInt8Array(interop::InteropInputStream* stream, int8_t* res, const int32_t len);

                /**
                 * Utility method to write signed 8-bit integer array to stream.
                 *
                 * @param stream Stream.
                 * @param val Value.
                 * @param len Array length.
                 */
                static void WriteInt8Array(interop::InteropOutputStream* stream, const int8_t* val, const int32_t len);

                /**
                 * Utility method to read boolean from stream.
                 *
                 * @param stream Stream.
                 * @return Value.
                 */
                static bool ReadBool(interop::InteropInputStream* stream);

                /**
                 * Utility method to write bool to stream.
                 *
                 * @param stream Stream.
                 * @param val Value.
                 */
                static void WriteBool(interop::InteropOutputStream* stream, bool val);

                /**
                 * Utility method to read bool array from stream.
                 *
                 * @param stream Stream.
                 * @param res Target array.
                 * @param len Array length.
                 */
                static void ReadBoolArray(interop::InteropInputStream* stream, bool* res, const int32_t len);

                /**
                 * Utility method to write bool array to stream.
                 *
                 * @param stream Stream.
                 * @param val Value.
                 * @param len Array length.
                 */
                static void WriteBoolArray(interop::InteropOutputStream* stream, const bool* val, const int32_t len);

                /**
                 * Utility method to read signed 16-bit integer from stream.
                 *
                 * @param stream Stream.
                 * @return Value.
                 */
                static int16_t ReadInt16(interop::InteropInputStream* stream);

                /**
                 * Utility method to read signed 16-bit integer from memory.
                 * @throw IgniteError if there is not enough memory.
                 *
                 * @param mem Memory.
                 * @param pos Position in memory.
                 * @return Value.
                 */
                static int16_t ReadInt16(interop::InteropMemory& mem, int32_t pos);

                /**
                 * Utility method to read signed 16-bit integer from memory.
                 * @warning Does not check if there is enough data in memory to read.
                 *
                 * @param mem Memory.
                 * @param pos Position in memory.
                 * @return Value.
                 */
                static int16_t UnsafeReadInt16(interop::InteropMemory& mem, int32_t pos);

                /**
                 * Utility method to write signed 16-bit integer to stream.
                 *
                 * @param stream Stream.
                 * @param val Value.
                 */
                static void WriteInt16(interop::InteropOutputStream* stream, int16_t val);

                /**
                 * Utility method to read signed 16-bit integer array from stream.
                 *
                 * @param stream Stream.
                 * @param res Target array.
                 * @param len Array length.                 
                 */
                static void ReadInt16Array(interop::InteropInputStream* stream, int16_t* res, const int32_t len);

                /**
                 * Utility method to write signed 16-bit integer array to stream.
                 *
                 * @param stream Stream.
                 * @param val Value.
                 * @param len Array length.
                 */
                static void WriteInt16Array(interop::InteropOutputStream* stream, const int16_t* val, const int32_t len);

                /**
                 * Utility method to read unsigned 16-bit integer from stream.
                 *
                 * @param stream Stream.
                 * @return Value.
                 */
                static uint16_t ReadUInt16(interop::InteropInputStream* stream);

                /**
                 * Utility method to write unsigned 16-bit integer to stream.
                 *
                 * @param stream Stream.
                 * @param val Value.
                 */
                static void WriteUInt16(interop::InteropOutputStream* stream, uint16_t val);

                /**
                 * Utility method to read unsigned 16-bit integer array from stream.
                 *
                 * @param stream Stream.
                 * @param res Target array.
                 * @param len Array length.
                 */
                static void ReadUInt16Array(interop::InteropInputStream* stream, uint16_t* res, const int32_t len);

                /**
                 * Utility method to write unsigned 16-bit integer array to stream.
                 *
                 * @param stream Stream.
                 * @param val Value.
                 * @param len Array length.
                 */
                static void WriteUInt16Array(interop::InteropOutputStream* stream, const uint16_t* val, const int32_t len);

                /**
                 * Utility method to read signed 32-bit integer from stream.
                 *
                 * @param stream Stream.
                 * @return Value.
                 */
                static int32_t ReadInt32(interop::InteropInputStream* stream);

                /**
                 * Utility method to read signed 32-bit integer from memory.
                 * @throw IgniteError if there is not enough memory.
                 *
                 * @param mem Memory.
                 * @param pos Position in memory.
                 * @return Value.
                 */
                static int32_t ReadInt32(interop::InteropMemory& mem, int32_t pos);

                /**
                 * Utility method to read signed 32-bit integer from memory.
                 * @warning Does not check if there is enough data in memory to read.
                 *
                 * @param mem Memory.
                 * @param pos Position in memory.
                 * @return Value.
                 */
                static int32_t UnsafeReadInt32(interop::InteropMemory& mem, int32_t pos);

                /**
                 * Utility method to write signed 32-bit integer to stream.
                 *
                 * @param stream Stream.
                 * @param val Value.
                 */
                static void WriteInt32(interop::InteropOutputStream* stream, int32_t val);

                /**
                 * Utility method to read signed 32-bit integer array from stream.
                 *
                 * @param stream Stream.
                 * @param res Target array.
                 * @param len Array length.
                 */
                static void ReadInt32Array(interop::InteropInputStream* stream, int32_t* res, const int32_t len);

                /**
                 * Utility method to write signed 32-bit integer array to stream.
                 *
                 * @param stream Stream.
                 * @param val Value.
                 * @param len Array length.
                 */
                static void WriteInt32Array(interop::InteropOutputStream* stream, const int32_t* val, const int32_t len);

                /**
                 * Utility method to read signed 64-bit integer from stream.
                 *
                 * @param stream Stream.
                 * @return Value.
                 */
                static int64_t ReadInt64(interop::InteropInputStream* stream);

                /**
                 * Utility method to write signed 64-bit integer to stream.
                 *
                 * @param stream Stream.
                 * @param val Value.
                 */
                static void WriteInt64(interop::InteropOutputStream* stream, int64_t val);

                /**
                 * Utility method to read signed 64-bit integer array from stream.
                 *
                 * @param stream Stream.
                 * @param res Target array.
                 * @param len Array length.
                 */
                static void ReadInt64Array(interop::InteropInputStream* stream, int64_t* res, const int32_t len);

                /**
                 * Utility method to write signed 64-bit integer array to stream.
                 *
                 * @param stream Stream.
                 * @param val Value.
                 * @param len Array length.
                 */
                static void WriteInt64Array(interop::InteropOutputStream* stream, const int64_t* val, const int32_t len);

                /**
                 * Utility method to read float from stream.
                 *
                 * @param stream Stream.
                 * @return Value.
                 */
                static float ReadFloat(interop::InteropInputStream* stream);

                /**
                 * Utility method to write float to stream.
                 *
                 * @param stream Stream.
                 * @param val Value.
                 */
                static void WriteFloat(interop::InteropOutputStream* stream, float val);

                /**
                 * Utility method to read float array from stream.
                 *
                 * @param stream Stream.
                 * @param res Target array.
                 * @param len Array length.
                 */
                static void ReadFloatArray(interop::InteropInputStream* stream, float* res, const int32_t len);

                /**
                 * Utility method to write float array to stream.
                 *
                 * @param stream Stream.
                 * @param val Value.
                 * @param len Array length.
                 */
                static void WriteFloatArray(interop::InteropOutputStream* stream, const float* val, const int32_t len);

                /**
                 * Utility method to read double from stream.
                 *
                 * @param stream Stream.
                 * @return Value.
                 */
                static double ReadDouble(interop::InteropInputStream* stream);

                /**
                 * Utility method to write double to stream.
                 *
                 * @param stream Stream.
                 * @param val Value.
                 */
                static void WriteDouble(interop::InteropOutputStream* stream, double val);

                /**
                 * Utility method to read double array from stream.
                 *
                 * @param stream Stream.
                 * @param res Target array.
                 * @param len Array length.
                 */
                static void ReadDoubleArray(interop::InteropInputStream* stream, double* res, const int32_t len);

                /**
                 * Utility method to write double array to stream.
                 *
                 * @param stream Stream.
                 * @param val Value.
                 * @param len Array length.
                 */
                static void WriteDoubleArray(interop::InteropOutputStream* stream, const double* val, const int32_t len);

                /**
                 * Utility method to read Guid from stream.
                 *
                 * @param stream Stream.
                 * @param res Value.
                 */
                static Guid ReadGuid(interop::InteropInputStream* stream);

                /**
                 * Utility method to write Guid to stream.
                 *
                 * @param stream Stream.
                 * @param val Value.
                 */
                static void WriteGuid(interop::InteropOutputStream* stream, const Guid val);

                /**
                 * Utility method to read Date from stream.
                 *
                 * @param stream Stream.
                 * @param res Value.
                 */
                static Date ReadDate(interop::InteropInputStream* stream);

                /**
                 * Utility method to write Date to stream.
                 *
                 * @param stream Stream.
                 * @param val Value.
                 */
                static void WriteDate(interop::InteropOutputStream* stream, const Date val);

                /**
                 * Utility method to read Timestamp from stream.
                 *
                 * @param stream Stream.
                 * @param res Value.
                 */
                static Timestamp ReadTimestamp(interop::InteropInputStream* stream);

                /**
                 * Utility method to write Timestamp to stream.
                 *
                 * @param stream Stream.
                 * @param val Value.
                 */
                static void WriteTimestamp(interop::InteropOutputStream* stream, const Timestamp val);

                /**
                 * Utility method to write string to stream.
                 *
                 * @param stream Stream.
                 * @param val Value.
                 * @param len Length.
                 */
                static void WriteString(interop::InteropOutputStream* stream, const char* val, const int32_t len);

                /**
                 * Get default value for the type.
                 *
                 * @return Null value for non primitive types and zeroes for primitives.
                 */
                template<typename T>
                static T GetDefaultValue()
                {
                    ignite::binary::BinaryType<T> binType;

                    return binType.GetNull();
                }
            };

            template<>
            inline int8_t BinaryUtils::GetDefaultValue<int8_t>()
            {
                return 0;
            }

            template<>
            inline int16_t BinaryUtils::GetDefaultValue<int16_t>()
            {
                return 0;
            }

            template<>
            inline uint16_t BinaryUtils::GetDefaultValue<uint16_t>()
            {
                return 0;
            }

            template<>
            inline int32_t BinaryUtils::GetDefaultValue<int32_t>()
            {
                return 0;
            }

            template<>
            inline int64_t BinaryUtils::GetDefaultValue<int64_t>()
            {
                return 0;
            }

            template<>
            inline bool BinaryUtils::GetDefaultValue<bool>()
            {
                return false;
            }

            template<>
            inline float BinaryUtils::GetDefaultValue<float>()
            {
                return 0.0f;
            }

            template<>
            inline double BinaryUtils::GetDefaultValue<double>()
            {
                return 0.0;
            }

            template<>
            inline Guid BinaryUtils::GetDefaultValue<Guid>()
            {
                return Guid();
            }

            template<>
            inline Date BinaryUtils::GetDefaultValue<Date>()
            {
                return Date();
            }

            template<>
            inline Timestamp BinaryUtils::GetDefaultValue<Timestamp>()
            {
                return Timestamp();
            }

            template<>
            inline std::string BinaryUtils::GetDefaultValue<std::string>()
            {
                return std::string();
            }
        }
    }
}

#endif //_IGNITE_IMPL_BINARY_BINARY_UTILS
