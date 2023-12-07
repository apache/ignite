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

#ifndef _IGNITE_ODBC__ENGINE_MODE_H
#define _IGNITE_ODBC__ENGINE_MODE_H

#include <set>
#include <string>

namespace ignite
{
    namespace odbc
    {
        /**
         * SQL engine mode.
         */
        struct EngineMode
        {
            enum Type
            {
                H2 = 0,

                CALCITE = 1,

                DEFAULT = 100,

                UNKNOWN = 101
            };

            /** Mode set type. */
            typedef std::set<Type> ModeSet;

            /**
             * Get value from a string value.
             *
             * @param val String value.
             * @param dflt Value to return on error.
             * @return Corresponding value on success and @c dflt on failure.
             */
            static Type FromString(const std::string& str, Type dflt = UNKNOWN);

            /**
             * Convert value to a string.
             *
             * @param val Value.
             * @return String value.
             */
            static std::string ToString(Type val);

            /**
             * Get set of all valid values.
             *
             * @return Set of all valid values.
             */
            static const ModeSet& GetValidValues();
        };
    }
}
#endif //_IGNITE_ODBC__ENGINE_MODE_H
