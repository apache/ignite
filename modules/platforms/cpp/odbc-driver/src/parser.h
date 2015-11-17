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

#ifndef _IGNITE_ODBC_DRIVER_PARSER
#define _IGNITE_ODBC_DRIVER_PARSER

#include <stdint.h>

#include <vector>

#include <ignite/impl/interop/interop_output_stream.h>
#include <ignite/impl/interop/interop_input_stream.h>
#include <ignite/impl/binary/binary_writer_impl.h>
#include <ignite/impl/binary/binary_reader_impl.h>

namespace ignite
{
    namespace odbc
    {
        /**
         * Message parser.
         */
        class Parser
        {
        public:
            /** Default initial size of operational memory. */
            enum { DEFAULT_MEM_ALLOCATION = 4096 };

            /**
             * Constructor.
             */
            Parser(int32_t cap = DEFAULT_MEM_ALLOCATION) : mem(cap), outStream(&mem), inStream(&mem)
            {
                //No-op.
            }

            /**
             * Destructor.
             */
            ~Parser()
            {
                //No-op.
            }

            /**
             * Encode message and place encoded data in buffer.
             *
             * @param msg Message to encode.
             * @param buf Data buffer.
             */
            template<typename MsgT>
            void Encode(const MsgT& msg, std::vector<int8_t>& buf)
            {
                using namespace ignite::impl::binary;

                ResetState();

                BinaryWriterImpl writer(&outStream, 0);

                int32_t msgLenPos = outStream.Reserve(4);

                msg.Write(writer);

                outStream.WriteInt32(msgLenPos, outStream.Position() - 4);

                buf.resize(outStream.Position());

                memcpy(&buf[0], mem.Data(), outStream.Position());
            }

            /**
             * Decode message from data in buffer.
             *
             * @param msg Message to decode.
             * @param buf Data buffer.
             */
            template<typename MsgT>
            void Decode(MsgT& msg, const std::vector<int8_t>& buf)
            {
                using namespace ignite::impl::binary;

                ResetState();

                // Saving memory internal state.
                int8_t* memDataMemo = mem.Data();
                int32_t memCapasityMemo = mem.Capacity();

                // Setting new state.
                mem.Data(&buf[0]);
                mem.Length(static_cast<int32_t>(buf.size()));
                mem.Capacity(static_cast<int32_t>(buf.capacity()));

                BinaryReaderImpl reader(&inStream);

                msg.Read(reader);

                // Restoring state.
                mem.Data(memDataMemo);
                mem.Length(0);
                mem.Capacity(memCapasityMemo);
            }

        private:
            /**
             * Reset internal state of the parser.
             */
            void ResetState()
            {
                mem.Length(0);

                outStream.Position(0);

                inStream.Position(0);
            }

            /** Operational memory. */
            ignite::impl::interop::InteropUnpooledMemory mem;

            /** Output stream. */
            ignite::impl::interop::InteropOutputStream outStream;

            /** Input stream. */
            ignite::impl::interop::InteropInputStream inStream;
        };
    }
}

#endif