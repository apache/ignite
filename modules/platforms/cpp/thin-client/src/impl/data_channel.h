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

#ifndef _IGNITE_IMPL_THIN_DATA_CHANNEL
#define _IGNITE_IMPL_THIN_DATA_CHANNEL

#include <stdint.h>

#include <memory>

#include <ignite/thin/ignite_client_configuration.h>

#include <ignite/common/concurrent.h>
#include <ignite/network/socket_client.h>

#include <ignite/impl/interop/interop_output_stream.h>
#include <ignite/impl/binary/binary_writer_impl.h>

#include "impl/protocol_version.h"
#include "impl/ignite_node.h"

namespace ignite
{
    namespace impl
    {
        namespace interop
        {
            // Forward declaration.
            class InteropMemory;
        }

        namespace thin
        {
            /**
             * Data router.
             *
             * Ensures there is a connection between client and one of the servers
             * and routes data between them.
             */
            class DataChannel
            {
            public:
                /** Version set type. */
                typedef std::set<ProtocolVersion> VersionSet;

                /** Version 1.2.0. */
                static const ProtocolVersion VERSION_1_2_0;

                /** Version 1.3.0. */
                static const ProtocolVersion VERSION_1_3_0;
                
                /** Version 1.4.0. Added: Partition awareness support, IEP-23. */
                static const ProtocolVersion VERSION_1_4_0;

                /** Version 1.5.0. Transaction support. */
                static const ProtocolVersion VERSION_1_5_0;

                /** Version 1.6.0. Expiration Policy Configuration. */
                static const ProtocolVersion VERSION_1_6_0;

                /** Version 1.6.0. Features introduced. */
                static const ProtocolVersion VERSION_1_7_0;

                /** Current version. */
                static const ProtocolVersion VERSION_DEFAULT;

                /**
                 * Operation with timeout result.
                 */
                struct OperationResult
                {
                    enum T
                    {
                        SUCCESS,
                        FAIL,
                        TIMEOUT
                    };
                };

                /**
                 * Constructor.
                 *
                 * @param cfg Configuration.
                 * @param typeMgr Type manager.
                 */
                DataChannel(const ignite::thin::IgniteClientConfiguration& cfg, binary::BinaryTypeManager& typeMgr);

                /**
                 * Destructor.
                 */
                ~DataChannel();

                /**
                 * Establish connection to cluster.
                 *
                 * @param host Host.
                 * @param port Port.
                 * @param timeout Timeout.
                 * @return @c true on success.
                 */
                bool Connect(const std::string& host, uint16_t port, int32_t timeout);

                /**
                 * Close connection.
                 */
                void Close();

                /**
                 * Synchronously send request message and receive response.
                 * Uses provided timeout. Does not try to restore connection on
                 * fail.
                 *
                 * @param req Request message.
                 * @param rsp Response message.
                 * @param timeout Timeout.
                 * @throw IgniteError on error.
                 */
                template<typename ReqT, typename RspT>
                void SyncMessage(const ReqT& req, RspT& rsp, int32_t timeout)
                {
                    // Allocating 64KB to lessen number of reallocations.
                    enum { BUFFER_SIZE = 1024 * 64 };

                    interop::InteropUnpooledMemory mem(BUFFER_SIZE);

                    int64_t id = GenerateRequestMessage(req, mem);

                    InternalSyncMessage(mem, timeout);

                    interop::InteropInputStream inStream(&mem);

                    inStream.Position(4);

                    int64_t rspId = inStream.ReadInt64();

                    if (id != rspId)
                        throw IgniteError(IgniteError::IGNITE_ERR_GENERIC,
                            "Protocol error: Response message ID does not equal Request ID");

                    binary::BinaryReaderImpl reader(&inStream);

                    rsp.Read(reader, currentVersion);
                }

                /**
                 * Send message stored in memory and synchronously receives
                 * response and stores it in the same memory.
                 *
                 * @param mem Memory.
                 * @param timeout Operation timeout.
                 */
                void InternalSyncMessage(interop::InteropUnpooledMemory& mem, int32_t timeout);

                /**
                 * Get remote node.
                 * @return Node.
                 */
                const IgniteNode& GetNode() const
                {
                    return node;
                }

                /**
                 * Check if the connection established.
                 *
                 * @return @true if connected.
                 */
                bool IsConnected() const
                {
                    return socket.get() != 0;
                }

            private:
                IGNITE_NO_COPY_ASSIGNMENT(DataChannel);

                /**
                 * Generate request ID.
                 *
                 * Atomically generates and returns new Request ID.
                 *
                 * @return Unique Request ID.
                 */
                int64_t GenerateRequestId()
                {
                    return common::concurrent::Atomics::IncrementAndGet64(&reqIdCounter);
                }

                /**
                 * Generate message to send.
                 *
                 * @param req Request to serialize.
                 * @param mem Memory to write request to.
                 * @return Message ID.
                 */
                template<typename ReqT>
                int64_t GenerateRequestMessage(const ReqT& req, interop::InteropUnpooledMemory& mem)
                {
                    interop::InteropOutputStream outStream(&mem);
                    binary::BinaryWriterImpl writer(&outStream, &typeMgr);

                    // Space for RequestSize + OperationCode + RequestID.
                    outStream.Reserve(4 + 2 + 8);

                    req.Write(writer, currentVersion);

                    int64_t id = GenerateRequestId();

                    outStream.WriteInt32(0, outStream.Position() - 4);
                    outStream.WriteInt16(4, ReqT::GetOperationCode());
                    outStream.WriteInt64(6, id);

                    outStream.Synchronize();

                    return id;
                }

                /**
                 * Send data by established connection.
                 *
                 * @param data Data buffer.
                 * @param len Data length.
                 * @param timeout Timeout.
                 * @return @c true on success, @c false on timeout.
                 * @throw IgniteError on error.
                 */
                bool Send(const int8_t* data, size_t len, int32_t timeout);

                /**
                 * Receive next message.
                 *
                 * @param msg Buffer for message.
                 * @param timeout Timeout.
                 * @return @c true on success, @c false on timeout.
                 * @throw IgniteError on error.
                 */
                bool Receive(interop::InteropMemory& msg, int32_t timeout);

                /**
                 * Receive specified number of bytes.
                 *
                 * @param dst Buffer for data.
                 * @param len Number of bytes to receive.
                 * @param timeout Timeout.
                 * @return Operation result.
                 */
                OperationResult::T ReceiveAll(void* dst, size_t len, int32_t timeout);

                /**
                 * Send specified number of bytes.
                 *
                 * @param data Data buffer.
                 * @param len Data length.
                 * @param timeout Timeout.
                 * @return Operation result.
                 */
                OperationResult::T SendAll(const int8_t* data, size_t len, int32_t timeout);

                /**
                 * Perform handshake request.
                 *
                 * @param propVer Proposed protocol version.
                 * @param resVer Resulted version.
                 * @param timeout Timeout.
                 * @return @c true on success and @c false otherwise.
                 */
                bool MakeRequestHandshake(const ProtocolVersion& propVer, ProtocolVersion& resVer, int32_t timeout);

                /**
                 * Synchronously send handshake request message and receive
                 * handshake response. Uses provided timeout. Does not try to
                 * restore connection on fail.
                 *
                 * @param propVer Proposed protocol version.
                 * @param resVer Resulted version.
                 * @param timeout Timeout.
                 * @return @c true if accepted.
                 * @throw IgniteError on error.
                 */
                bool Handshake(const ProtocolVersion& propVer, ProtocolVersion& resVer, int32_t timeout);

                /**
                 * Ensure there is a connection to the cluster.
                 *
                 * @param timeout Timeout.
                 * @return @c false on error.
                 */
                bool EnsureConnected(int32_t timeout);

                /**
                 * Negotiate protocol version with current host
                 *
                 * @param timeout Timeout.
                 * @return @c true on success and @c false otherwise.
                 */
                bool NegotiateProtocolVersion(int32_t timeout);

                /**
                 * Try to restore connection to the cluster.
                 *
                 * @param timeout Timeout.
                 * @return @c true on success and @c false otherwise.
                 */
                bool TryRestoreConnection(int32_t timeout);

                /**
                 * Check if the version is supported.
                 *
                 * @param ver Version.
                 * @return True if the version is supported.
                 */
                static bool IsVersionSupported(const ProtocolVersion& ver);

                /** Set of supported versions. */
                const static VersionSet supportedVersions;

                /** Sync IO mutex. */
                common::concurrent::CriticalSection ioMutex;

                /** Remote node data. */
                IgniteNode node;

                /** Configuration. */
                const ignite::thin::IgniteClientConfiguration& config;

                /** Metadata manager. */
                binary::BinaryTypeManager& typeMgr;

                /** Protocol version. */
                ProtocolVersion currentVersion;

                /** Request ID counter. */
                int64_t reqIdCounter;

                /** Client Socket. */
                std::auto_ptr<network::SocketClient> socket;
            };

            /** Shared pointer type. */
            typedef common::concurrent::SharedPointer<DataChannel> SP_DataChannel;
        }
    }
}

#endif //_IGNITE_IMPL_THIN_DATA_CHANNEL
