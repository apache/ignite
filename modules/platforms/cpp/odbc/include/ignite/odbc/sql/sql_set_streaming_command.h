/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
        class SqlToken;

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

            /**
             * Check if the streaming enabled.
             *
             * @return @c true if enabled.
             */
            bool IsEnabled() const
            {
                return enabled;
            }

            /**
             * Check if the overwrite is allowed.
             *
             * @return @c true if allowed.
             */
            bool IsAllowOverwrite() const
            {
                return allowOverwrite;
            }

            /**
             * Get batch size.
             *
             * @return Batch size.
             */
            int32_t GetBatchSize() const
            {
                return batchSize;
            }

            /**
             * Get parallel operations per node.
             *
             * @return Parallel operations per node.
             */
            int32_t GetParallelOperationsPerNode() const
            {
                return parallelOpsPerNode;
            }

            /**
             * Get buffer size per node.
             *
             * @return Buffer size per node.
             */
            int32_t GetBufferSizePerNode() const
            {
                return bufferSizePerNode;
            }

            /**
             * Get flush frequency.
             *
             * @return Flush frequency.
             */
            int64_t GetFlushFrequency() const
            {
                return flushFrequency;
            }

            /**
             * Check if the streaming is ordered.
             *
             * @return @c true if ordered.
             */
            bool IsOrdered() const
            {
                return ordered;
            }

        private:
            /**
             * Check that the streaming mode is enabled.
             */
            void CheckEnabled(const SqlToken& token) const;

            /**
             * Throw exception, showing that token is unexpected.
             *
             * @param token Token.
             * @param expected Expected details.
             */
            static void ThrowUnexpectedTokenError(const SqlToken& token, const std::string& expected);

            /**
             * Throw exception, showing that token is unexpected.
             *
             * @param expected Expected details.
             */
            static void ThrowUnexpectedEndOfStatement(const std::string& expected);

            /**
             * Get int or throw parsing exception.
             *
             * @param lexer Lexer to use.
             * @return Integer number.
             */
            static int32_t ExpectInt(SqlLexer& lexer);

            /**
             * Get positive int or throw parsing exception.
             *
             * @param lexer Lexer to use.
             * @param description Param description to use in exception on error.
             * @return Integer number.
             */
            static int32_t ExpectPositiveInteger(SqlLexer& lexer, const std::string& description);

            /**
             * Get bool or throw parsing exception.
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