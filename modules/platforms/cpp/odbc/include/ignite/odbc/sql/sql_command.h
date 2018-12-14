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

#ifndef _IGNITE_ODBC_SQL_SQL_COMMAND
#define _IGNITE_ODBC_SQL_SQL_COMMAND

namespace ignite
{
    namespace odbc
    {
        class SqlLexer;

        /**
         * SQL command type.
         */
        struct SqlCommandType
        {
            enum Type
            {
                SET_STREAMING
            };
        };

        /**
         * SQL command.
         */
        class SqlCommand
        {
        public:
            /**
             * Constructor.
             *
             * @param typ Type.
             */
            SqlCommand(SqlCommandType::Type typ) :
                typ(typ)
            {
                // No-op.
            }

            /**
             * Destructor.
             */
            virtual ~SqlCommand()
            {
                // No-op.
            }

            /**
             * Get type.
             *
             * @return Type.
             */
            SqlCommandType::Type GetType() const
            {
                return typ;
            }

            /**
             * Parse from lexer.
             *
             * @param lexer Lexer.
             */
            virtual void Parse(SqlLexer& lexer) = 0;

        protected:
            /** Type. */
            SqlCommandType::Type typ;
        };
    }
}

#endif //_IGNITE_ODBC_SQL_SQL_COMMAND