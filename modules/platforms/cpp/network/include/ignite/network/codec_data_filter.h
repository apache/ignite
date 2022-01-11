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

#ifndef _IGNITE_NETWORK_CODEC_DATA_FILTER
#define _IGNITE_NETWORK_CODEC_DATA_FILTER

#include <map>

#include <ignite/common/concurrent.h>

#include <ignite/network/codec.h>
#include <ignite/network/data_filter_adapter.h>

namespace ignite
{
    namespace network
    {
        /**
         * Data filter that uses codecs inside to encode/decode data.
         */
        class IGNITE_IMPORT_EXPORT CodecDataFilter : public DataFilterAdapter
        {
        public:
            /**
             * Constructor.
             *
             * @param factory Codec factory.
             */
            explicit CodecDataFilter(const SP_CodecFactory& factory);

            /**
             * Destructor.
             */
            virtual ~CodecDataFilter();

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
            /** Codec map. */
            typedef std::map<uint64_t, SP_Codec> CodecMap;

            /**
             * Get codec for connection.
             *
             * @param id Connection ID.
             * @return Codec if found or null.
             */
            SP_Codec FindCodec(uint64_t id);

            /** Codec factory. */
            SP_CodecFactory codecFactory;

            /** Codecs. */
            CodecMap* codecs;

            /** Mutex for secure access to codecs map. */
            common::concurrent::CriticalSection codecsCs;
        };

        // Shared pointer type alias.
        typedef common::concurrent::SharedPointer<CodecDataFilter> SP_CodecDataFilter;
    }
}

#endif //_IGNITE_NETWORK_CODEC_DATA_FILTER
