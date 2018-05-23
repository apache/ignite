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

#include <ignite/impl/thin/parser.h>
#include <ignite/impl/thin/net/end_point.h>
#include <ignite/impl/thin/socket_client.h>
#include "ignite/thin/ignite_client_configuration.h"

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            class Statement;

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
                bool Receive(std::vector<int8_t>& msg, int32_t timeout);

                /**
                 * Synchronously send request message and receive response.
                 * Uses provided timeout.
                 *
                 * @param req Request message.
                 * @param rsp Response message.
                 * @param timeout Timeout.
                 * @return @c true on success, @c false on timeout.
                 * @throw IgniteError on error.
                 */
                template<typename ReqT, typename RspT>
                bool SyncMessage(const ReqT& req, RspT& rsp, int32_t timeout)
                {
                    EnsureConnected();

                    return InternalSyncMessage(req, rsp, timeout);
                }

            private:
                IGNITE_NO_COPY_ASSIGNMENT(DataRouter);

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
                    std::vector<int8_t> tempBuffer;

                    parser.Encode(req, tempBuffer);

                    bool success = Send(tempBuffer.data(), tempBuffer.size(), timeout);

                    if (!success)
                        return false;

                    success = Receive(tempBuffer, timeout);

                    if (!success)
                        return false;

                    parser.Decode(rsp, tempBuffer);

                    return true;
                }

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

                /** Client Socket. */
                std::auto_ptr<SocketClient> socket;

                /** Connection timeout in seconds. */
                int32_t timeout;

                /** Login timeout in seconds. */
                int32_t loginTimeout;

                /** Message parser. */
                Parser parser;

                /** Set of supported versions. */
                const static VersionSet supportedVersions;
                
                /** Configuration. */
                ignite::thin::IgniteClientConfiguration config;
            };
        }
    }
}

#endif //_IGNITE_IMPL_THIN_DATA_ROUTER
