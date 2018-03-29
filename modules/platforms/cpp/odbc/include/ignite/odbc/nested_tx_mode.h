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

#ifndef _IGNITE_ODBC_NESTED_TX_MODE
#define _IGNITE_ODBC_NESTED_TX_MODE

#include <set>
#include <string>

namespace ignite
{
    namespace odbc
    {
        /**
         * Nested transaction mode.
         */
        struct NestedTxMode
        {
            /**
             * Values.
             */
            enum Type
            {
                /** Commit current transaction if a new one started. */
                AI_COMMIT = 1,

                /** Ignore start of a new transaction. */
                AI_IGNORE = 2,

                /** Throw an error. */
                AI_ERROR = 3,

                /** Returned when value is unknown. */
                AI_UNKNOWN = 100
            };

            /** Mode set type. */
            typedef std::set<Type> ModeSet;

            /**
             * Get value from a string value.
             *
             * @param str String.
             * @param dflt Value to return on error.
             * @return Corresponding value on success and @c dflt on failure.
             */
            static Type FromString(const std::string& str, Type dflt = AI_UNKNOWN);

            /**
             * Convert value to a string.
             *
             * @param value Value.
             * @return String value.
             */
            static std::string ToString(Type value);

            /**
             * Get set of all valid values.
             *
             * @return Set of all valid values.
             */
            static const ModeSet& GetValidValues();
        };
    }
}

#endif //_IGNITE_ODBC_NESTED_TX_MODE