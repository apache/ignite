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

#ifndef _IGNITE_ODBC_SQL_SQL_SET_STREAMING_COMMAND
#define _IGNITE_ODBC_SQL_SQL_SET_STREAMING_COMMAND

#include <stdint.h>
#include <string>

#include <ignite/odbc/sql/sql_command.h>

namespace ignite
{
    namespace odbc
    {
        /**
         * SQL set streaming command.
         */
        class SqlSetStreamingCommand : public SqlCommand
        {
            /** Default batch size for driver. */
            enum { DEFAULT_STREAM_BATCH_SIZE = 2048 };

        public:
            /**
             * Default constructor.
             */
            SqlSetStreamingCommand();

            /**
             * Destructor.
             */
            virtual ~SqlSetStreamingCommand();

            /**
             * Parse from lexer.
             *
             * @param lexer Lexer.
             */
            virtual void Parse(SqlLexer& lexer);

        private:
            /**
             * Get int or throw parsring exception.
             *
             * @param lexer Lexer to use.
             * @return Integer number.
             */
            static int32_t ExpectInt(SqlLexer& lexer);

            /**
             * Get positive int or throw parsring exception.
             *
             * @param lexer Lexer to use.
             * @param description Param description to use in exception on error.
             * @return Integer number.
             */
            static int32_t ExpectPositiveInteger(SqlLexer& lexer, const std::string& description);

            /**
             * Get bool or throw parsring exception.
             *
             * @param lexer Lexer to use.
             * @return Boolean value.
             */
            static bool ExpectBool(SqlLexer& lexer);

            /** Whether streaming must be turned on or off by this command. */
            bool enabled;

            /** Whether existing values should be overwritten on keys duplication. */
            bool allowOverwrite;

            /** Batch size for driver. */
            int32_t batchSize;

            /** Per node number of parallel operations. */
            int32_t parallelOpsPerNode;

            /** Per node buffer size. */
            int32_t bufferSizePerNode;

            /** Streamer flush timeout. */
            int64_t flushFrequency;

            /** Ordered streamer. */
            bool ordered;
        };
    }
}

#endif //_IGNITE_ODBC_SQL_SQL_SET_STREAMING_COMMAND