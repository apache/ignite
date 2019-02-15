/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

#ifndef _IGNITE_ODBC_PARSER
#define _IGNITE_ODBC_PARSER

#include <stdint.h>

#include <vector>

#include <ignite/impl/interop/interop_output_stream.h>
#include <ignite/impl/interop/interop_input_stream.h>
#include <ignite/impl/binary/binary_writer_impl.h>
#include <ignite/impl/binary/binary_reader_impl.h>

#include "ignite/odbc/utility.h"
#include "ignite/odbc/protocol_version.h"

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
             * @param cap Initial capasity.
             */
            Parser(int32_t cap = DEFAULT_MEM_ALLOCATION) :
                protocolVer(ProtocolVersion::GetCurrent()),
                inMem(cap),
                outMem(cap),
                outStream(&outMem)
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
                using namespace impl::binary;

                ResetState();

                BinaryWriterImpl writer(&outStream, 0);

                msg.Write(writer, protocolVer);

                buf.resize(outStream.Position());

                memcpy(&buf[0], outMem.Data(), outStream.Position());
            }

            /**
             * Decode message from data in buffer.
             *
             * @param msg Message to decode.
             * @param buf Data buffer.
             * @note Can be optimized after InteropMemory refactoring.
             */
            template<typename MsgT>
            void Decode(MsgT& msg, const std::vector<int8_t>& buf)
            {
                using namespace impl::binary;

                if (inMem.Capacity() < static_cast<int32_t>(buf.size()))
                    inMem.Reallocate(static_cast<int32_t>(buf.size()));

                memcpy(inMem.Data(), buf.data(), buf.size());

                inMem.Length(static_cast<int32_t>(buf.size()));

                impl::interop::InteropInputStream inStream(&inMem);

                BinaryReaderImpl reader(&inStream);

                msg.Read(reader, protocolVer);
            }

            /**
             * Set protocol version.
             *
             * @param ver Version to set.
             */
            void SetProtocolVersion(const ProtocolVersion& ver)
            {
                protocolVer = ver;
            }

        private:
            IGNITE_NO_COPY_ASSIGNMENT(Parser);

            /**
             * Reset internal state of the parser.
             */
            void ResetState()
            {
                outMem.Length(0);

                outStream.Position(0);
            }

            /** Protocol version. */
            ProtocolVersion protocolVer;

            /** Input operational memory. */
            impl::interop::InteropUnpooledMemory inMem;

            /** Output operational memory. */
            impl::interop::InteropUnpooledMemory outMem;

            /** Output stream. */
            impl::interop::InteropOutputStream outStream;
        };
    }
}

#endif //_IGNITE_ODBC_PARSER