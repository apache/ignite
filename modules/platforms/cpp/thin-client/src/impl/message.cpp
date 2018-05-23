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

#include <ignite/impl/thin/response_status.h>
#include <ignite/impl/thin/message.h>

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            HandshakeRequest::HandshakeRequest(const ignite::thin::IgniteClientConfiguration& config) :
                config(config)
            {
                // No-op.
            }

            HandshakeRequest::~HandshakeRequest()
            {
                // No-op.
            }

            void HandshakeRequest::Write(binary::BinaryWriterImpl& writer, const ProtocolVersion& ver) const
            {
                writer.WriteInt8(RequestType::HANDSHAKE);

                writer.WriteInt16(ver.GetMajor());
                writer.WriteInt16(ver.GetMinor());
                writer.WriteInt16(ver.GetMaintenance());

                writer.WriteInt8(ClientType::THIN_CLIENT);

                writer.WriteString(config.GetUser());
                writer.WriteString(config.GetPassword());
            }

            HandshakeResponse::HandshakeResponse():
                accepted(false),
                currentVer(),
                error(),
                status(ResponseStatus::FAILED)
            {
                // No-op.
            }

            HandshakeResponse::~HandshakeResponse()
            {
                // No-op.
            }

            void HandshakeResponse::Read(binary::BinaryReaderImpl& reader, const ProtocolVersion&)
            {
                accepted = reader.ReadBool();
                
                if (!accepted)
                {
                    int16_t major = reader.ReadInt16();
                    int16_t minor = reader.ReadInt16();
                    int16_t maintenance = reader.ReadInt16();

                    currentVer = ProtocolVersion(major, minor, maintenance);

                    reader.ReadString(error);

                    status = reader.ReadInt32();
                }
            }
        }
    }
}

