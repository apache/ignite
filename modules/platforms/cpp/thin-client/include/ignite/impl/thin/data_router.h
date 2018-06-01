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

#ifndef _IGNITE_IMPL_THIN_DATA_ROUTER
#define _IGNITE_IMPL_THIN_DATA_ROUTER

#include <stdint.h>

#include <vector>

#include <ignite/common/concurrent.h>

#include <ignite/impl/interop/interop_output_stream.h>
#include <ignite/impl/binary/binary_writer_impl.h>

#include <ignite/impl/thin/protocol_version.h>
#include <ignite/impl/thin/net/end_point.h>
#include <ignite/impl/thin/socket_client.h>

#include <ignite/thin/ignite_client_configuration.h>

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
            class DataRouter
            {
            public:
                /** Version 1.1.0. */
                static const ProtocolVersion VERSION_1_1_0;

                /** Current version. */
                static const ProtocolVersion VERSION_CURRENT;

                typedef std::set<ProtocolVersion> VersionSet;

                /** Connection establishment timeout in seconds. */
                enum { DEFALT_CONNECT_TIMEOUT = 5 };

                /** Network IO operation timeout in seconds. */
                enum { DEFALT_IO_TIMEOUT = 5 };

                /** Default port. */
                enum { DEFAULT_PORT = 10800 };

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
                 */
                DataRouter(const ignite::thin::IgniteClientConfiguration& cfg);

                /**
                 * Destructor.
                 */
                ~DataRouter();

                /**
                 * Establish connection to cluster.
                 */
                void Connect();

                /**
                 * Close connection.
                 */
                void Close();

                /**
                 * Synchronously send request message and receive response.
                 * Uses provided timeout.
                 *
                 * @param req Request message.
                 * @param rsp Response message.
                 * @return @c true on success, @c false on timeout.
                 * @throw IgniteError on error.
                 */
                template<typename ReqT, typename RspT>
                bool SyncMessage(const ReqT& req, RspT& rsp)
                {
                    EnsureConnected();

                    return InternalSyncMessage(req, rsp, ioTimeout);
                }

            private:
                IGNITE_NO_COPY_ASSIGNMENT(DataRouter);

                /**
                 * Generate request ID.
                 *
                 * Atomicaly generates and returns new Request ID.
                 *
                 * @return Unique Request ID.
                 */
                int64_t GenerateRequestId()
                {
                    return common::concurrent::Atomics::IncrementAndGet64(&reqIdCounter);
                }

                /**
                 * Synchronously send request message and receive response.
                 * Uses provided timeout. Does not try to restore connection on
                 * fail.
                 *
                 * @param req Request message.
                 * @param rsp Response message.
                 * @param timeout Timeout.
                 * @return @c true on success, @c false on timeout.
                 * @throw IgniteError on error.
                 */
                template<typename ReqT, typename RspT>
                bool InternalSyncMessage(const ReqT& req, RspT& rsp, int32_t timeout)
                {
                    // Allocating 64KB to lessen number of reallocations.
                    enum { BUFFER_SIZE = 1024 * 64 };

                    interop::InteropUnpooledMemory mem(BUFFER_SIZE);
                    interop::InteropOutputStream outStream(&mem);
                    binary::BinaryWriterImpl writer(&outStream, 0);

                    // Space for RequestSize + OperationCode + RequestID.
                    outStream.Reserve(4 + 2 + 8);

                    req.Write(writer, currentVersion);

                    int64_t id = GenerateRequestId();

                    outStream.WriteInt32(0, outStream.Position() - 4);
                    outStream.WriteInt16(4, ReqT::GetOperationCode());
                    outStream.WriteInt64(6, id);

                    { // Locking scope
                        common::concurrent::CsLockGuard lock(ioMutex);

                        bool success = Send(mem.Data(), outStream.Position(), timeout);

                        if (!success)
                            throw IgniteError(IgniteError::IGNITE_ERR_GENERIC,
                                "Can not send message to remote host: timeout");

                        success = Receive(mem, timeout);

                        if (!success)
                            throw IgniteError(IgniteError::IGNITE_ERR_GENERIC,
                                "Can not send message to remote host: timeout");
                    }

                    interop::InteropInputStream inStream(&mem);

                    inStream.Position(4);

                    int32_t rspId = inStream.ReadInt8();

                    if (id != rspId)
                        throw IgniteError(IgniteError::IGNITE_ERR_GENERIC,
                            "Protocol error: Response message ID does not equal Request ID");

                    binary::BinaryReaderImpl reader(&inStream);

                    rsp.Read(reader, currentVersion);

                    return true;
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
                 * @return @c true on success and @c false otherwise.
                 */
                bool MakeRequestHandshake(const ProtocolVersion& propVer, ProtocolVersion& resVer);

                /**
                 * Synchronously send handshake request message and receive
                 * handshake response. Uses provided timeout. Does not try to
                 * restore connection on fail.
                 *
                 * @param propVer Proposed protocol version.
                 * @param resVer Resulted version.
                 * @return @c true if accepted.
                 * @throw IgniteError on error.
                 */
                bool Handshake(const ProtocolVersion& propVer, ProtocolVersion& resVer);

                /**
                 * Ensure there is a connection to the cluster.
                 *
                 * @throw IgniteError on failure.
                 */
                void EnsureConnected();

                /**
                 * Negotiate protocol version with current host
                 *
                 * @return @c true on success and @c false otherwise.
                 */
                bool NegotiateProtocolVersion();

                /**
                 * Try to restore connection to the cluster.
                 *
                 * @return @c true on success and @c false otherwise.
                 */
                bool TryRestoreConnection();

                /**
                 * Collect all addresses from string.
                 *
                 * @param str String with connection strings to parse.
                 * @param endPoints End points.
                 */
                static void CollectAddresses(const std::string& str, std::vector<net::EndPoint>& endPoints);

                /**
                 * Check if the version is supported.
                 *
                 * @param ver Version.
                 * @return True if the version is supported.
                 */
                static bool IsVersionSupported(const ProtocolVersion& ver);

                /** Set of supported versions. */
                const static VersionSet supportedVersions;

                /** IO timeout in seconds. */
                int32_t ioTimeout;

                /** Connection timeout in seconds. */
                int32_t connectionTimeout;

                /** Client Socket. */
                std::auto_ptr<SocketClient> socket;

                /** Configuration. */
                ignite::thin::IgniteClientConfiguration config;

                /** Protocol version. */
                ProtocolVersion currentVersion;

                /** Request ID counter. */
                int64_t reqIdCounter;

                /** Sync IO mutex. */
                common::concurrent::CriticalSection ioMutex;
            };

            /** Shared pointer type. */
            typedef common::concurrent::SharedPointer<DataRouter> SP_DataRouter;
        }
    }
}

#endif //_IGNITE_IMPL_THIN_DATA_ROUTER
