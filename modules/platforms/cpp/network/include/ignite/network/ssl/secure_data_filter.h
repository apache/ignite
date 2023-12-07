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

#ifndef _IGNITE_NETWORK_SSL_SECURE_DATA_FILTER
#define _IGNITE_NETWORK_SSL_SECURE_DATA_FILTER

#include <map>

#include <ignite/common/concurrent.h>
#include <ignite/network/data_filter_adapter.h>
#include <ignite/network/ssl/secure_configuration.h>

namespace ignite
{
    namespace network
    {
        namespace ssl
        {
            /**
             * TLS/SSL Data Filter.
             */
            class IGNITE_IMPORT_EXPORT SecureDataFilter : public DataFilterAdapter
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param cfg Configuration.
                 */
                explicit SecureDataFilter(const SecureConfiguration& cfg);

                /**
                 * Destructor.
                 */
                virtual ~SecureDataFilter();

                /**
                 * Send data to specific established connection.
                 *
                 * @param id Client ID.
                 * @param data Data to be sent.
                 * @return @c true if connection is present and @c false otherwise.
                 *
                 * @throw IgniteError on error.
                 */
                virtual bool Send(uint64_t id, const DataBuffer& data);

                /**
                  * Callback that called on successful connection establishment.
                  *
                  * @param addr Address of the new connection.
                  * @param id Connection ID.
                  */
                virtual void OnConnectionSuccess(const EndPoint& addr, uint64_t id);

                /**
                 * Callback that called on error during connection establishment.
                 *
                 * @param id Async client ID.
                 * @param err Error. Can be null if connection closed without error.
                 */
                virtual void OnConnectionClosed(uint64_t id, const IgniteError* err);

                /**
                 * Callback that called when new message is received.
                 *
                 * @param id Async client ID.
                 * @param msg Received message.
                 */
                virtual void OnMessageReceived(uint64_t id, const DataBuffer& msg);

            private:
                IGNITE_NO_COPY_ASSIGNMENT(SecureDataFilter);

                /**
                 * Secure connection context.
                 */
                class SecureConnectionContext
                {
                public:
                    /**
                     * Default constructor.
                     *
                     * @param id Connection ID.
                     * @param addr Address.
                     * @param filter Filter.
                     */
                    SecureConnectionContext(uint64_t id, const EndPoint &addr, SecureDataFilter& filter);

                    /**
                     * Destructor.
                     */
                    ~SecureConnectionContext();

                    /**
                     * Start connection procedure including handshake.
                     *
                     * @return @c true, if connection complete.
                     */
                    bool DoConnect();

                    /**
                     * Check whether connection is established.
                     *
                     * @return @c true if connection established.
                     */
                    bool IsConnected() const
                    {
                        return connected;
                    }

                    /**
                     * Get address.
                     *
                     * @return Address.
                     */
                    const EndPoint& GetAddress() const
                    {
                        return addr;
                    }

                    /**
                     * Send data.
                     *
                     * @param data Data to send.
                     * @return @c true on success.
                     */
                    bool Send(const DataBuffer& data);

                    /**
                     * Process new data.
                     *
                     * @param data Data received.
                     * @return @c true if connection was established.
                     */
                    bool ProcessData(DataBuffer& data);

                    /**
                     * Get pending decrypted data.
                     *
                     * @return Data buffer.
                     */
                    DataBuffer GetPendingDecryptedData();

                private:
                    enum
                    {
                        /** Receive buffer size. */
                        RECEIVE_BUFFER_SIZE = 0x10000
                    };

                    /**
                     * Send pending data.
                     */
                    bool SendPendingData();

                    /**
                     * Get pending data.
                     *
                     * @param bio BIO to get data from.
                     * @return Data buffer.
                     */
                    static DataBuffer GetPendingData(void* bio);

                    /** Flag indicating that secure connection is established. */
                    bool connected;

                    /** Connection ID. */
                    const uint64_t id;

                    /** Address. */
                    const EndPoint addr;

                    /** Filter. */
                    SecureDataFilter& filter;

                    /** Receive buffer. */
                    impl::interop::SP_InteropMemory recvBuffer;

                    /** SSL instance. */
                    void* ssl;

                    /** Input BIO. */
                    void* bioIn;

                    /** Output BIO. */
                    void* bioOut;
                };

                // Shared pointer type alias.
                typedef common::concurrent::SharedPointer<SecureConnectionContext> SP_SecureConnectionContext;

                /** Context map. */
                typedef std::map<uint64_t, SP_SecureConnectionContext> ContextMap;

                /**
                 * Get context for connection.
                 *
                 * @param id Connection ID.
                 * @return Context if found or null.
                 */
                SP_SecureConnectionContext FindContext(uint64_t id);

                /**
                 * Send data to specific established connection.
                 *
                 * @param id Client ID.
                 * @param data Data to be sent.
                 * @return @c true if connection is present and @c false otherwise.
                 *
                 * @throw IgniteError on error.
                 */
                bool SendInternal(uint64_t id, const DataBuffer& data);

                /** SSL context. */
                void* sslContext;

                /** Contexts for connections. */
                ContextMap* contexts;

                /** Mutex for secure access to context map. */
                common::concurrent::CriticalSection contextCs;
            };

            // Shared pointer type alias.
            typedef common::concurrent::SharedPointer<SecureDataFilter> SP_SecureDataFilter;
        }
    }
}

#endif //_IGNITE_NETWORK_SSL_SECURE_DATA_FILTER
