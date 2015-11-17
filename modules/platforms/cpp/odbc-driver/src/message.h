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

#ifndef _IGNITE_ODBC_DRIVER_MESSAGE
#define _IGNITE_ODBC_DRIVER_MESSAGE

#include <stdint.h>
#include <string>

#include "ignite/impl/binary/binary_writer_impl.h"

namespace ignite
{
    namespace odbc
    {
        enum RequestType
        {
            REQUEST_TYPE_EXECUTE_SQL_QUERY = 1,

            REQUEST_TYPE_FETCH_SQL_QUERY = 2,

            REQUEST_TYPE_CLOSE_SQL_QUERY = 3
        };

        enum ResponseStatus
        {
            RESPONSE_STATUS_SUCCESS = 0,

            RESPONSE_STATUS_FAILED = 1
        };

        /**
         * Query execute request.
         */
        class QueryExecuteRequest
        {
        public:
            /**
             * Constructor.
             */
            QueryExecuteRequest(const std::string& cache, const std::string& sql, size_t argsNum = 0) :
                cache(cache), sql(sql) 
            {
                // No-op.
            }

            /**
             * Destructor.
             */
            ~QueryExecuteRequest()
            {
                // No-op.
            }

            /**
             * Write request using provided writer.
             * @param writer Writer.
             */
            void Write(ignite::impl::binary::BinaryWriterImpl& writer) const
            {
                writer.WriteInt8(REQUEST_TYPE_EXECUTE_SQL_QUERY);
                writer.WriteString(cache.c_str(), static_cast<int32_t>(cache.size()));
                writer.WriteString(sql.c_str(), static_cast<int32_t>(sql.size()));
                writer.WriteInt32(0);
            }

        private:
            /** Cache name. */
            std::string cache;

            /** SQL query. */
            std::string sql;
        };

        /**
         * Query execute response.
         */
        class QueryExecuteResponse
        {
        public:
            /**
             * Constructor.
             */
            QueryExecuteResponse() : queryId(0)
            {
                // No-op.
            }

            /**
             * Destructor.
             */
            ~QueryExecuteResponse()
            {
                // No-op.
            }

            /**
             * Read response using provided reader.
             * @param reader Reader.
             */
            void Read(ignite::impl::binary::BinaryReaderImpl& reader)
            {
                status = reader.ReadInt8();

                if (status == RESPONSE_STATUS_SUCCESS)
                    queryId = reader.ReadInt64();
            }

            /**
             * Get query ID.
             * @return Query ID.
             */
            int64_t GetQueryId() const
            {
                return queryId;
            }

            /**
             * Get request processing status.
             * @return Status.
             */
            int8_t GetStatus() const
            {
                return status;
            }

        private:
            /** Request processing status. */
            int8_t status;

            /** Query ID. */
            int64_t queryId;
        };
    }
}

#endif