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

#include <cstring>

#include <ignite/ignite_error.h>

#include "ignite/impl/interop//interop_output_stream.h"

namespace ignite
{
    namespace impl
    {
        namespace interop
        {
            class OutputStreamHelper
            {
            public:
                explicit OutputStreamHelper(InteropOutputStream& os0) : os(os0)
                {
                    // No-op;
                };

                template<typename T>
                void WritePrimitiveAt(const T& val, int32_t pos)
                {
                    os.EnsureCapacity(pos + sizeof(T));
                    std::memcpy(os.data + pos, &val, sizeof(T));
                }

                template<typename T>
                void WritePrimitive(const T& val)
                {
                    os.EnsureCapacity(os.pos + sizeof(T));
                    std::memcpy(os.data + os.pos, &val, sizeof(T));
                    os.Shift(sizeof(T));
                }

                template<typename T>
                void WritePrimitiveArray(const T* arr, int32_t len)
                {
                    os.CopyAndShift(arr, 0, len * sizeof(T));
                }

            private:
                InteropOutputStream& os;
            };

            InteropOutputStream::InteropOutputStream(InteropMemory* mem)
            {
                this->mem = mem;

                data = mem->Data();
                cap = mem->Capacity();
                pos = 0;
            }

            void InteropOutputStream::WriteInt8(int8_t val)
            {
                OutputStreamHelper(*this).WritePrimitive<int8_t>(val);
            }

            void InteropOutputStream::WriteInt8(int8_t val, int32_t pos0)
            {
                OutputStreamHelper(*this).WritePrimitiveAt<int8_t>(val, pos0);
            }

            void InteropOutputStream::WriteInt8Array(const int8_t* val, int32_t len)
            {
                OutputStreamHelper(*this).WritePrimitiveArray<int8_t>(val, len);
            }

            void InteropOutputStream::WriteBool(bool val)
            {
                WriteInt8(val ? 1 : 0);
            }

            void InteropOutputStream::WriteBoolArray(const bool* val, int32_t len)
            {
                for (int i = 0; i < len; i++)
                    WriteBool(*(val + i));
            }

            void InteropOutputStream::WriteInt16(const int16_t val)
            {
                OutputStreamHelper(*this).WritePrimitive<int16_t>(val);
            }

            void InteropOutputStream::WriteInt16(int32_t pos0, int16_t val)
            {
                OutputStreamHelper(*this).WritePrimitiveAt<int16_t>(val, pos0);
            }

            void InteropOutputStream::WriteInt16Array(const int16_t* val, int32_t len)
            {
                OutputStreamHelper(*this).WritePrimitiveArray<int16_t>(val, len);
            }

            void InteropOutputStream::WriteUInt16(uint16_t val)
            {
                OutputStreamHelper(*this).WritePrimitive<uint16_t>(val);
            }

            void InteropOutputStream::WriteUInt16Array(const uint16_t* val, int32_t len)
            {
                OutputStreamHelper(*this).WritePrimitiveArray<uint16_t>(val, len);
            }

            void InteropOutputStream::WriteInt32(int32_t val)
            {
                OutputStreamHelper(*this).WritePrimitive<int32_t>(val);
            }

            void InteropOutputStream::WriteInt32(int32_t pos0, int32_t val)
            {
                OutputStreamHelper(*this).WritePrimitiveAt<int32_t>(val, pos0);
            }

            void InteropOutputStream::WriteInt32Array(const int32_t* val, int32_t len)
            {
                OutputStreamHelper(*this).WritePrimitiveArray<int32_t>(val, len);
            }

            void InteropOutputStream::WriteInt64(int64_t val)
            {
                OutputStreamHelper(*this).WritePrimitive<int64_t>(val);
            }

            void InteropOutputStream::WriteInt64(const int32_t pos0, const int64_t val)
            {
                OutputStreamHelper(*this).WritePrimitiveAt<int64_t>(val, pos0);
            }

            void InteropOutputStream::WriteInt64Array(const int64_t* val, int32_t len)
            {
                OutputStreamHelper(*this).WritePrimitiveArray<int64_t>(val, len);
            }

            void InteropOutputStream::WriteFloat(float val)
            {
                BinaryFloatInt32 u = {val};

                WriteInt32(u.i);
            }

            void InteropOutputStream::WriteFloatArray(const float* val, int32_t len)
            {
                for (int i = 0; i < len; i++)
                    WriteFloat(*(val + i));
            }

            void InteropOutputStream::WriteDouble(double val)
            {
                BinaryDoubleInt64 u = {val};

                WriteInt64(u.i);
            }

            void InteropOutputStream::WriteDoubleArray(const double* val, int32_t len)
            {
                for (int i = 0; i < len; i++)
                    WriteDouble(*(val + i));
            }

            int32_t InteropOutputStream::Position() const
            {
                return pos;
            }

            void InteropOutputStream::Position(int32_t val)
            {
                EnsureCapacity(val);

                pos = val;
            }

            int32_t InteropOutputStream::Reserve(int32_t num)
            {
                EnsureCapacity(pos + num);

                int32_t res = pos;

                Shift(num);

                return res;
            }

            void InteropOutputStream::Synchronize()
            {
                mem->Length(pos);
            }

            InteropMemory* InteropOutputStream::GetMemory()
            {
                return mem;
            }

            void InteropOutputStream::EnsureCapacity(int32_t reqCap) {
                if (reqCap > cap) {
                    int newCap = cap << 1;

                    if (newCap < reqCap)
                        newCap = reqCap;

                    mem->Reallocate(newCap);
                    data = mem->Data();
                    cap = newCap;
                }
            }

            void InteropOutputStream::Shift(int32_t cnt) {
                pos += cnt;
            }

            void InteropOutputStream::CopyAndShift(const void* src, int32_t off, int32_t len) {
                EnsureCapacity(pos + len);

                memcpy(data + pos, reinterpret_cast<const int8_t*>(src) + off, len);

                Shift(len);
            }
        }
    }
}
