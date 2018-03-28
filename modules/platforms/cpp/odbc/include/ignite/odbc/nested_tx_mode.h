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

#include <string>

namespace ignite
{
    namespace odbc
    {
        struct NestedTxMode
        {
            enum Type
            {
                AI_COMMIT = 1,

                AI_IGNORE = 2,

                AI_ERROR = 3,

                AI_UNKNOWN = 100
            };

            static Type FromString(const std::string& str, Type dflt = AI_UNKNOWN);
        };
    }
}

#endif //_IGNITE_ODBC_NESTED_TX_MODE