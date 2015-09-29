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

#ifndef _IGNITE_IMPL_INTEROP_INPUT_STREAM
#define _IGNITE_IMPL_INTEROP_INPUT_STREAM

#include "ignite/impl/interop/interop_memory.h"

namespace ignite
{    
    namespace impl
    {
        namespace interop
        {
            /**
             * Interop input stream implementation.
             */
            class IGNITE_IMPORT_EXPORT InteropInputStream {
            public:
                /**
                 * Constructor.
                 *
                 * @param mem Memory.
                 */
                InteropInputStream(InteropMemory* mem);

                /**
                 * Read signed 8-byte int.
                 *
                 * @return Value.
                 */
                int8_t ReadInt8();
                    
                /**
                 * Read signed 8-byte int array.
                 *
                 * @param res Allocated array.
                 * @param len Length.                 
                 */
                void ReadInt8Array(int8_t* const res, const int32_t len);

                /**
                 * Read bool.
                 *
                 * @return Value.
                 */
                bool ReadBool();

                /**
                 * Read bool array.
                 *
                 * @param res Allocated array.
                 * @param len Length.
                 */
                void ReadBoolArray(bool* const res, const int32_t len);

                /**
                 * Read signed 16-byte int.
                 *
                 * @return Value.
                 */
                int16_t ReadInt16();

                /**
                 * Read signed 16-byte int array.
                 *
                 * @param res Allocated array.
                 * @param len Length.
                 */
                void ReadInt16Array(int16_t* const res, const int32_t len);

                /**
                 * Read unsigned 16-byte int.
                 *
                 * @return Value.
                 */
                uint16_t ReadUInt16();

                /**
                 * Read unsigned 16-byte int array.
                 *
                 * @param res Allocated array.
                 * @param len Length.
                 */
                void ReadUInt16Array(uint16_t* const res, const int32_t len);

                /**
                 * Read signed 32-byte int.
                 *
                 * @return Value.
                 */
                int32_t ReadInt32();

                /**
                 * Read signed 32-byte int at the given position.
                 *
                 * @param pos Position.
                 * @return Value.
                 */
                int32_t ReadInt32(int32_t pos);
                    
                /**
                 * Read signed 32-byte int array.
                 *
                 * @param res Allocated array.
                 * @param len Length.
                 */
                void ReadInt32Array(int32_t* const res, const int32_t len);

                /**
                 * Read signed 64-byte int.
                 *
                 * @return Value.
                 */
                int64_t ReadInt64();

                /**
                 * Read signed 64-byte int array.
                 *
                 * @param res Allocated array.
                 * @param len Length.
                 */
                void ReadInt64Array(int64_t* const res, const int32_t len);

                /**
                 * Read float.
                 *
                 * @return Value.
                 */
                float ReadFloat();

                /**
                 * Read float array.
                 *
                 * @param res Allocated array.
                 * @param len Length.
                 */
                void ReadFloatArray(float* const res, const int32_t len);

                /**
                 * Read double.
                 *
                 * @return Value.
                 */
                double ReadDouble();

                /**
                 * Read double array.
                 *
                 * @param res Allocated array.
                 * @param len Length.
                 */
                void ReadDoubleArray(double* const res, const int32_t len);

                /**
                 * Get remaining bytes.
                 *
                 * @return Remaining bytes.
                 */
                int32_t Remaining();

                /**
                 * Get position.
                 *
                 * @return Position.
                 */
                int32_t Position();

                /**
                 * Set position.
                 *
                 * @param Position.
                 */
                void Position(int32_t pos);

                /**
                 * Synchronize data from underlying memory.
                 */
                void Synchronize();
            private:
                /** Memory. */
                InteropMemory* mem; 
                
                /** Pointer to data. */
                int8_t* data;       
                
                /** Length. */
                int len;            
                
                /** Current position. */
                int pos;            
                    
                /**
                 * Ensure there is enough data in the stream.
                 *
                 * @param cnt Amount of byte expected to be available.
                 */
                void EnsureEnoughData(int32_t cnt);

                /**
                 * Copy data from the stream shifting it along the way.
                 *
                 * @param ptr Pointer to data.
                 * @param off Offset.
                 * @param cnt Amount of data to copy.
                 */
                void CopyAndShift(int8_t* dest, int32_t off, int32_t cnt);

                /**
                 * Shift stream to the right.
                 *
                 * @param cnt Amount of bytes to shift the stream to.
                 */
                void Shift(int32_t cnt);
            };
        }
    }    
}

#endif