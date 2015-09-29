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

#include "ignite/impl/interop/interop_input_stream.h"
#include "ignite/ignite_error.h"

/**
 * Common macro to read a single value.
 */
#define IGNITE_INTEROP_IN_READ(type, len) { \
    EnsureEnoughData(len); \
    type res = *reinterpret_cast<type*>(data + pos); \
    Shift(len); \
    return res; \
}

/**
 * Common macro to read an array.
 */
#define IGNITE_INTEROP_IN_READ_ARRAY(len, shift) { \
    CopyAndShift(reinterpret_cast<int8_t*>(res), 0, len << shift); \
}

namespace ignite
{    
    namespace impl
    {
        namespace interop 
        {
            union PortableInt32Float
            {
                int32_t i;
                float f;
            };

            union PortableInt64Double
            {
                int64_t i;
                double d;
            };

            InteropInputStream::InteropInputStream(InteropMemory* mem)
            {
                this->mem = mem;

                data = mem->Data();
                len = mem->Length();
                pos = 0;
            }

            int8_t InteropInputStream::ReadInt8()
            {
                IGNITE_INTEROP_IN_READ(int8_t, 1);
            }

            void InteropInputStream::ReadInt8Array(int8_t* const res, const int32_t len)
            {
                IGNITE_INTEROP_IN_READ_ARRAY(len, 0);
            }

            bool InteropInputStream::ReadBool()
            {
                return ReadInt8() == 1;
            }

            void InteropInputStream::ReadBoolArray(bool* const res, const int32_t len)
            {
                for (int i = 0; i < len; i++)
                    *(res + i) = ReadBool();
            }
                
            int16_t InteropInputStream::ReadInt16()
            {
                IGNITE_INTEROP_IN_READ(int16_t, 2);
            }

            void InteropInputStream::ReadInt16Array(int16_t* const res, const int32_t len)
            {
                IGNITE_INTEROP_IN_READ_ARRAY(len, 1);
            }

            uint16_t InteropInputStream::ReadUInt16()
            {
                IGNITE_INTEROP_IN_READ(uint16_t, 2);
            }

            void InteropInputStream::ReadUInt16Array(uint16_t* const res, const int32_t len)
            {
                IGNITE_INTEROP_IN_READ_ARRAY(len, 1);
            }

            int32_t InteropInputStream::ReadInt32()
            {
                IGNITE_INTEROP_IN_READ(int32_t, 4);
            }

            int32_t InteropInputStream::ReadInt32(int32_t pos)
            {
                int delta = pos + 4 - this->pos;

                if (delta > 0)
                    EnsureEnoughData(delta);

                return *reinterpret_cast<int32_t*>(data + pos);
            }

            void InteropInputStream::ReadInt32Array(int32_t* const res, const int32_t len)
            {
                IGNITE_INTEROP_IN_READ_ARRAY(len, 2);
            }

            int64_t InteropInputStream::ReadInt64()
            {
                IGNITE_INTEROP_IN_READ(int64_t, 8);
            }

            void InteropInputStream::ReadInt64Array(int64_t* const res, const int32_t len)
            {
                IGNITE_INTEROP_IN_READ_ARRAY(len, 3);
            }

            float InteropInputStream::ReadFloat()
            {
                PortableInt32Float u;

                u.i = ReadInt32();

                return u.f;
            }

            void InteropInputStream::ReadFloatArray(float* const res, const int32_t len)
            {
                IGNITE_INTEROP_IN_READ_ARRAY(len, 2);
            }

            double InteropInputStream::ReadDouble()
            {
                PortableInt64Double u;

                u.i = ReadInt64();

                return u.d;
            }

            void InteropInputStream::ReadDoubleArray(double* const res, const int32_t len)
            {
                IGNITE_INTEROP_IN_READ_ARRAY(len, 3);
            }
                
            int32_t InteropInputStream::Remaining()
            {
                return len - pos;
            }

            int32_t InteropInputStream::Position()
            {
                return pos;
            }

            void InteropInputStream::Position(int32_t pos)
            {
                if (pos > len) {
                    IGNITE_ERROR_FORMATTED_3(IgniteError::IGNITE_ERR_MEMORY, "Requested input stream position is out of bounds",
                        "memPtr", mem->PointerLong(), "len", len, "pos", pos);
                }

                this->pos = pos;
            }

            void InteropInputStream::Synchronize()
            {
                data = mem->Data();
                len = mem->Length();
            }
            
            void InteropInputStream::EnsureEnoughData(int32_t cnt)
            {
                if (len - pos < cnt) {
                    IGNITE_ERROR_FORMATTED_4(IgniteError::IGNITE_ERR_MEMORY, "Not enough data in the stream",
                        "memPtr", mem->PointerLong(), "len", len, "pos", pos, "requested", cnt);
                }
            }

            void InteropInputStream::CopyAndShift(int8_t* dest, int32_t off, int32_t cnt)
            {
                EnsureEnoughData(cnt);

                memcpy(dest + off, data + pos, cnt);

                Shift(cnt);
            }

            void InteropInputStream::Shift(int32_t cnt)
            {
                pos += cnt;
            }
        }
    }
}        
