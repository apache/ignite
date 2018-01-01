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

#ifndef _IGNITE_IMPL_INTEROP_INTEROP_OUTPUT_STREAM
#define _IGNITE_IMPL_INTEROP_INTEROP_OUTPUT_STREAM

#include "ignite/impl/interop/interop_memory.h"

namespace ignite
{    
    namespace impl
    {
        namespace interop
        {
            /**
             * Interop output stream.
             */
            class IGNITE_IMPORT_EXPORT InteropOutputStream {
            public:
                /**
                 * Create new output stream with the given capacity.
                 *
                 * @param mem Memory.
                 */
                InteropOutputStream(InteropMemory* mem);

                /**
                 * Write signed 8-byte integer.
                 *
                 * @param val Value.
                 */
                void WriteInt8(const int8_t val);

                /**
                 * Write signed 8-byte integer at the given position.
                 *
                 * @param val Value.
                 */
                void WriteInt8(const int8_t val, const int32_t pos);

                /**
                 * Write signed 8-byte integer array.
                 *
                 * @param val Value.
                 * @param len Length.
                 */
                void WriteInt8Array(const int8_t* val, const int32_t len);

                /**
                 * Write bool.
                 *
                 * @param val Value.
                 */
                void WriteBool(const bool val);

                /**
                 * Write bool array.
                 *
                 * @param val Value.
                 * @param len Length.
                 */
                void WriteBoolArray(const bool* val, const int32_t len);

                /**
                 * Write signed 16-byte integer.
                 *
                 * @param val Value.
                 */
                void WriteInt16(const int16_t val);

                /**
                 * Write signed 16-byte integer at the given position.
                 *
                 * @param pos Position.
                 * @param val Value.
                 */
                void WriteInt16(const int32_t pos, const int16_t val);

                /**
                 * Write signed 16-byte integer array.
                 *
                 * @param val Value.
                 * @param len Length.
                 */
                void WriteInt16Array(const int16_t* val, const int32_t len);

                /**
                 * Write unsigned 16-byte integer.
                 *
                 * @param val Value.
                 */
                void WriteUInt16(const uint16_t val);

                /**
                 * Write unsigned 16-byte integer array.
                 *
                 * @param val Value.
                 * @param len Length.
                 */
                void WriteUInt16Array(const uint16_t* val, const int32_t len);

                /**
                 * Write signed 32-byte integer.
                 *
                 * @param val Value.
                 */
                void WriteInt32(const int32_t val);

                /**
                 * Write signed 32-byte integer at the given position.
                 *
                 * @param pos Position.
                 * @param val Value.
                 */
                void WriteInt32(const int32_t pos, const int32_t val);

                /**
                 * Write signed 32-byte integer array.
                 *
                 * @param val Value.
                 * @param len Length.
                 */
                void WriteInt32Array(const int32_t* val, const int32_t len);

                /**
                 * Write signed 64-byte integer.
                 *
                 * @param val Value.
                 */
                void WriteInt64(const int64_t val);

                /**
                 * Write signed 64-byte integer array.
                 *
                 * @param val Value.
                 * @param len Length.
                 */
                void WriteInt64Array(const int64_t* val, const int32_t len);

                /**
                 * Write float.
                 *
                 * @param val Value.
                 */
                void WriteFloat(const float val);

                /**
                 * Write float array.
                 *
                 * @param val Value.
                 * @param len Length.
                 */
                void WriteFloatArray(const float* val, const int32_t len);

                /**
                 * Write double.
                 *
                 * @param val Value.
                 */
                void WriteDouble(const double val);

                /**
                 * Write double array.
                 *
                 * @param val Value.
                 * @param len Length.
                 */
                void WriteDoubleArray(const double* val, const int32_t len);

                /**
                 * Get current stream position.
                 */
                int32_t Position() const;

                /**
                 * Set current stream position (absolute).
                 *
                 * @param val Position (absolute).
                 */
                void Position(const int32_t val);

                /**
                 * Reserve specified number of bytes in stream.
                 *
                 * @param num Number of bytes to reserve.
                 * @return Absolute position to reserved space.
                 */
                int32_t Reserve(int32_t num);

                /**
                 * Synchronize data with underlying memory.
                 */
                void Synchronize();

                /**
                 * Get underlying memory.
                 *
                 * @return Underlying memory.
                 */
                InteropMemory* GetMemory();

            private:
                /** Memory. */
                InteropMemory* mem; 

                /** Pointer to data. */
                int8_t* data;       

                /** Capacity. */
                int cap;            

                /** Current position. */
                int pos;            

                IGNITE_NO_COPY_ASSIGNMENT(InteropOutputStream)

                /**
                 * Ensure that stream enough capacity optionally extending it.
                 *
                 * @param reqCap Requsted capacity.
                 */
                void EnsureCapacity(int32_t reqCap);

                /**
                 * Shift stream to the right.
                 *
                 * @param cnt Amount of bytes to shift the stream to.
                 */
                void Shift(int32_t cnt);

                /**
                 * Copy data to the stream shifting it along the way.
                 *
                 * @param src Pointer to data.
                 * @param off Offset.
                 * @param len Length.
                 */
                void CopyAndShift(const int8_t* src, int32_t off, int32_t len);
            };
        }
    }
}

#endif //_IGNITE_IMPL_INTEROP_INTEROP_OUTPUT_STREAM