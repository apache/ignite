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

#ifndef _IGNITE_IMPL_THIN_CACHE_QUERY_CONTINUOUS_CONTINUOUS_QUERY_NOTIFICATION_HANDLER
#define _IGNITE_IMPL_THIN_CACHE_QUERY_CONTINUOUS_CONTINUOUS_QUERY_NOTIFICATION_HANDLER

#include <ignite/common/common.h>
#include <ignite/common/concurrent.h>

#include <ignite/binary/binary_raw_reader.h>

#include <ignite/impl/interop/interop_input_stream.h>
#include <ignite/impl/binary/binary_reader_impl.h>

#include "impl/notification_handler.h"

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            namespace cache
            {
                namespace query
                {
                    namespace continuous
                    {
                        /**
                         * Continuous query notification handler.
                         */
                        class ContinuousQueryNotificationHandler : public NotificationHandler
                        {
                        public:
                            ContinuousQueryNotificationHandler(
                                const SP_ContinuousQueryClientHolderBase& continuousQuery) :
                                continuousQuery(continuousQuery)
                            {
                                // No-op.
                            }

                            /**
                             * Destructor.
                             */
                            virtual ~ContinuousQueryNotificationHandler()
                            {
                                // No-op.
                            }

                            /**
                             * Handle notification.
                             *
                             * @param msg Message.
                             * @return @c true if processing complete.
                             */
                            virtual bool OnNotification(const network::DataBuffer& msg)
                            {
                                interop::InteropInputStream in(msg.GetInputStream());

                                // TODO: Re-factor
                                // Skipping size (4 bytes) and reqId (8 bytes), flags and opcode
                                in.Ignore(16);

                                binary::BinaryReaderImpl readerImpl(&in);
                                ignite::binary::BinaryRawReader reader(&readerImpl);

                                continuousQuery.Get()->ReadAndProcessEvents(reader);

                                return false;
                            }

                        private:
                            /** Query. */
                            SP_ContinuousQueryClientHolderBase continuousQuery;
                        };

                        /** Shared pointer to ContinuousQueryHandleClientImpl. */
                        typedef common::concurrent::SharedPointer<ContinuousQueryNotificationHandler> SP_ContinuousQueryNotificationHandler;
                    }
                }
            }
        }
    }
}

#endif //_IGNITE_IMPL_THIN_CACHE_QUERY_CONTINUOUS_CONTINUOUS_QUERY_NOTIFICATION_HANDLER