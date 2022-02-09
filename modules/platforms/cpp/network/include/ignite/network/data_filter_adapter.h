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

#ifndef _IGNITE_NETWORK_DATA_FILTER_ADAPTER
#define _IGNITE_NETWORK_DATA_FILTER_ADAPTER

#include <ignite/network/data_filter.h>

namespace ignite
{
    namespace network
    {
        /**
         * Data buffer.
         */
        class IGNITE_IMPORT_EXPORT DataFilterAdapter : public DataFilter
        {
        public:
            /**
             * Default constructor.
             */
            DataFilterAdapter()
            {
                // No-op.
            }

            /**
             * Destructor.
             */
            virtual ~DataFilterAdapter()
            {
                // No-op.
            }

            /**
             * Send data to specific established connection.
             *
             * @param id Client ID.
             * @param data Data to be sent.
             * @return @c true if connection is present and @c false otherwise.
             *
             * @throw IgniteError on error.
             */
            virtual bool Send(uint64_t id, const DataBuffer& data)
            {
                DataSink* sink0 = sink;
                if (sink0)
                    return sink0->Send(id, data);

                return false;
            }

            /**
             * Closes specified connection if it's established. Connection to the specified address is planned for
             * re-connect. Error is reported to handler.
             *
             * @param id Client ID.
             */
            virtual void Close(uint64_t id, const IgniteError* err)
            {
                DataSink* sink0 = sink;
                if (sink0)
                    sink0->Close(id, err);
            }

            /**
              * Callback that called on successful connection establishment.
              *
              * @param addr Address of the new connection.
              * @param id Connection ID.
              */
            virtual void OnConnectionSuccess(const EndPoint& addr, uint64_t id)
            {
                AsyncHandler* handler0 = handler;
                if (handler0)
                    handler0->OnConnectionSuccess(addr, id);
            }

            /**
             * Callback that called on error during connection establishment.
             *
             * @param addr Connection address.
             * @param err Error.
             */
            virtual void OnConnectionError(const EndPoint& addr, const IgniteError& err)
            {
                AsyncHandler* handler0 = handler;
                if (handler0)
                    handler0->OnConnectionError(addr, err);
            }

            /**
             * Callback that called on error during connection establishment.
             *
             * @param id Async client ID.
             * @param err Error. Can be null if connection closed without error.
             */
            virtual void OnConnectionClosed(uint64_t id, const IgniteError* err)
            {
                AsyncHandler* handler0 = handler;
                if (handler0)
                    handler0->OnConnectionClosed(id, err);
            }

            /**
             * Callback that called when new message is received.
             *
             * @param id Async client ID.
             * @param msg Received message.
             */
            virtual void OnMessageReceived(uint64_t id, const DataBuffer& msg)
            {
                AsyncHandler* handler0 = handler;
                if (handler0)
                    handler0->OnMessageReceived(id, msg);
            }

            /**
             * Callback that called when message is sent.
             *
             * @param id Async client ID.
             */
            virtual void OnMessageSent(uint64_t id)
            {
                AsyncHandler* handler0 = handler;
                if (handler0)
                    handler0->OnMessageSent(id);
            }
        };
    }
}

#endif //_IGNITE_NETWORK_DATA_FILTER_ADAPTER
