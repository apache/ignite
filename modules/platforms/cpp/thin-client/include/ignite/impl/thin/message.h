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

#ifndef _IGNITE_IMPL_THIN_MESSAGE
#define _IGNITE_IMPL_THIN_MESSAGE

#include <stdint.h>
#include <string>

#include <ignite/impl/binary/binary_writer_impl.h>
#include <ignite/impl/binary/binary_reader_impl.h>

#include <ignite/impl/thin/protocol_version.h>
#include <ignite/thin/ignite_client_configuration.h>

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            struct ClientType
            {
                enum Type
                {
                    THIN_CLIENT = 2
                };
            };

            struct RequestType
            {
                enum Type
                {
                    HANDSHAKE = 1,
                };
            };

            /**
             * Handshake request.
             */
            class HandshakeRequest
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param config Configuration.
                 */
                HandshakeRequest(const ignite::thin::IgniteClientConfiguration& config);

                /**
                 * Destructor.
                 */
                ~HandshakeRequest();

                /**
                 * Write request using provided writer.
                 * @param writer Writer.
                 * @param ver Version.
                 */
                void Write(binary::BinaryWriterImpl& writer, const ProtocolVersion& ver) const;

            private:
                /** Configuration. */
                const ignite::thin::IgniteClientConfiguration& config;
            };

            /**
             * Handshake response.
             */
            class HandshakeResponse
            {
            public:
                /**
                 * Constructor.
                 */
                HandshakeResponse();

                /**
                 * Destructor.
                 */
                ~HandshakeResponse();

                /**
                 * Check if the handshake has been accepted.
                 * @return True if the handshake has been accepted.
                 */
                bool IsAccepted() const
                {
                    return accepted;
                }

                /**
                 * Current host Apache Ignite version.
                 * @return Current host Apache Ignite version.
                 */
                const ProtocolVersion& GetCurrentVer() const
                {
                    return currentVer;
                }

                /**
                 * Get optional error.
                 * @return Optional error message.
                 */
                const std::string& GetError() const
                {
                    return error;
                }

                /**
                 * Get request processing status.
                 * @return Status.
                 */
                int32_t GetStatus() const
                {
                    return status;
                }

                /**
                 * Read response using provided reader.
                 * @param reader Reader.
                 */
                void Read(binary::BinaryReaderImpl& reader, const ProtocolVersion&);

            private:
                /** Handshake accepted. */
                bool accepted;

                /** Node's protocol version. */
                ProtocolVersion currentVer;

                /** Optional error message. */
                std::string error;

                /** Request processing status. */
                int32_t status;
            };
        }
    }
}

#endif //_IGNITE_IMPL_THIN_MESSAGE
