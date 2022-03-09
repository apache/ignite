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

#ifndef _IGNITE_NETWORK_DATA_SINK
#define _IGNITE_NETWORK_DATA_SINK

#include <ignite/ignite_error.h>

#include <ignite/network/data_buffer.h>

namespace ignite
{
    namespace network
    {
        /**
         * Data sink. Can consume data.
         */
        class IGNITE_IMPORT_EXPORT DataSink
        {
        public:
            /**
             * Destructor.
             */
            virtual ~DataSink()
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
            virtual bool Send(uint64_t id, const DataBuffer& data) = 0;

            /**
             * Closes specified connection if it's established. Connection to the specified address is planned for
             * re-connect. Error is reported to handler.
             *
             * @param id Client ID.
             */
            virtual void Close(uint64_t id, const IgniteError* err) = 0;
        };
    }
}

#endif //_IGNITE_NETWORK_DATA_SINK
