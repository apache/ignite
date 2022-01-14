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

#include "ignite/impl/interop/interop_input_stream.h"

namespace ignite
{
    namespace impl
    {
        namespace interop
        {
            class InputStreamHelper
            {
            public:
                explicit InputStreamHelper(InteropInputStream& is0) : is(is0)
                {
                    // No-op;
                };

                template<typename T>
                T ReadPrimitiveAt(int32_t pos)
                {
                    int delta = pos + sizeof(T) - is.pos;

                    if (delta > 0)
                        is.EnsureEnoughData(delta);

                    T res;
                    std::memcpy(&res, is.data + pos, sizeof(T));
                    return res;
                }

                template<typename T>
                T ReadPrimitive()
                {
                    is.EnsureEnoughData(sizeof(T));

                    T res;
                    std::memcpy(&res, is.data + is.pos, sizeof(T));
                    is.Shift(sizeof(T));
                    return res;
                }

                template<typename T>
                void ReadPrimitiveArray(T* res, int32_t len)
                {
                    is.CopyAndShift(res, 0, len * sizeof(T));
                }

            private:
                InteropInputStream& is;
            };

            InteropInputStream::InteropInputStream(const InteropMemory* mem) :
                mem(mem),
                data(mem->Data()),
                len(mem->Length()),
                pos(0)
            {
                // No-op.
            }

            InteropInputStream::InteropInputStream(const InteropMemory *mem, int32_t len) :
                mem(mem),
                data(mem->Data()),
                len(len),
                pos(0)
            {
                if (len > mem->Length())
                    IGNITE_ERROR_FORMATTED_3(IgniteError::IGNITE_ERR_MEMORY,
                        "Requested input stream len is greater than memories length",
                             "memPtr", mem->PointerLong(), "len", len, "memLen", mem->Length());
            }

            int8_t InteropInputStream::ReadInt8()
            {
                return InputStreamHelper(*this).ReadPrimitive<int8_t>();
            }

            int8_t InteropInputStream::ReadInt8(int32_t pos)
            {
                return InputStreamHelper(*this).ReadPrimitiveAt<int8_t>(pos);
            }

            void InteropInputStream::ReadInt8Array(int8_t* res, int32_t len)
            {
                return InputStreamHelper(*this).ReadPrimitiveArray<int8_t>(res, len);
            }

            bool InteropInputStream::ReadBool()
            {
                return ReadInt8() == 1;
            }

            void InteropInputStream::ReadBoolArray(bool* res, int32_t len)
            {
                for (int i = 0; i < len; i++)
                    *(res + i) = ReadBool();
            }

            int16_t InteropInputStream::ReadInt16()
            {
                return InputStreamHelper(*this).ReadPrimitive<int16_t>();
            }

            int16_t InteropInputStream::ReadInt16(int32_t pos)
            {
                return InputStreamHelper(*this).ReadPrimitiveAt<int16_t>(pos);
            }

            void InteropInputStream::ReadInt16Array(int16_t* res, int32_t len)
            {
                return InputStreamHelper(*this).ReadPrimitiveArray<int16_t>(res, len);
            }

            uint16_t InteropInputStream::ReadUInt16()
            {
                return InputStreamHelper(*this).ReadPrimitive<uint16_t>();
            }

            void InteropInputStream::ReadUInt16Array(uint16_t* res, int32_t len)
            {
                return InputStreamHelper(*this).ReadPrimitiveArray<uint16_t>(res, len);
            }

            int32_t InteropInputStream::ReadInt32()
            {
                return InputStreamHelper(*this).ReadPrimitive<int32_t>();
            }

            int32_t InteropInputStream::ReadInt32(int32_t pos)
            {
                return InputStreamHelper(*this).ReadPrimitiveAt<int32_t>(pos);
            }

            void InteropInputStream::ReadInt32Array(int32_t* res, int32_t len)
            {
                return InputStreamHelper(*this).ReadPrimitiveArray<int32_t>(res, len);
            }

            int64_t InteropInputStream::ReadInt64()
            {
                return InputStreamHelper(*this).ReadPrimitive<int64_t>();
            }

            void InteropInputStream::ReadInt64Array(int64_t* res, int32_t len)
            {
                return InputStreamHelper(*this).ReadPrimitiveArray<int64_t>(res, len);
            }

            float InteropInputStream::ReadFloat()
            {
                BinaryFloatInt32 u;

                u.i = ReadInt32();

                return u.f;
            }

            void InteropInputStream::ReadFloatArray(float* res, int32_t len)
            {
                return InputStreamHelper(*this).ReadPrimitiveArray<float>(res, len);
            }

            double InteropInputStream::ReadDouble()
            {
                BinaryDoubleInt64 u;

                u.i = ReadInt64();

                return u.d;
            }

            void InteropInputStream::ReadDoubleArray(double* res, int32_t len)
            {
                return InputStreamHelper(*this).ReadPrimitiveArray<double>(res, len);
            }

            int32_t InteropInputStream::Remaining() const
            {
                return len - pos;
            }

            int32_t InteropInputStream::Position() const
            {
                return pos;
            }

            void InteropInputStream::Position(int32_t pos)
            {
                if (pos <= len)
                    this->pos = pos;
                else {
                    IGNITE_ERROR_FORMATTED_3(IgniteError::IGNITE_ERR_MEMORY, "Requested input stream position is out of bounds",
                        "memPtr", mem->PointerLong(), "len", len, "pos", pos);
                }
            }

            void InteropInputStream::Ignore(int32_t cnt)
            {
                Shift(cnt);
            }

            void InteropInputStream::Synchronize()
            {
                data = mem->Data();
                len = mem->Length();
            }

            void InteropInputStream::EnsureEnoughData(int32_t cnt) const
            {
                if (len - pos >= cnt)
                    return;
                else {
                    IGNITE_ERROR_FORMATTED_4(IgniteError::IGNITE_ERR_MEMORY, "Not enough data in the stream",
                        "memPtr", mem->PointerLong(), "len", len, "pos", pos, "requested", cnt);
                }
            }

            void InteropInputStream::CopyAndShift(void* dest, int32_t off, int32_t cnt)
            {
                EnsureEnoughData(cnt);

                if (dest != 0)
                    std::memcpy(static_cast<int8_t*>(dest) + off, data + pos, cnt);

                Shift(cnt);
            }

            inline void InteropInputStream::Shift(int32_t cnt)
            {
                pos += cnt;
            }
        }
    }
}
