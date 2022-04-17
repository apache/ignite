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

#ifndef _IGNITE_IMPL_THIN_RESPONSE_STATUS
#define _IGNITE_IMPL_THIN_RESPONSE_STATUS

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            struct ResponseStatus
            {
                enum Type
                {
                    /** Operation completed successfully. */
                    SUCCESS = 0,

                    /** Command failed. */
                    FAILED = 1,

                    /** Invalid op code. */
                    INVALID_OP_CODE = 2,

                    /** Cache does not exist. */
                    CACHE_DOES_NOT_EXIST = 1000,

                    /** Cache already exists. */
                    CACHE_EXISTS = 1001,

                    /** Too many cursors. */
                    TOO_MANY_CURSORS = 1010,

                    /** Resource does not exist. */
                    RESOURCE_DOES_NOT_EXIST = 1011,

                    /** Authorization failure. */
                    SECURITY_VIOLATION = 1012,

                    /** Authentication failed. */
                    AUTH_FAILED = 2000
                };
            };
        }
    }
}

#endif //_IGNITE_IMPL_THIN_RESPONSE_STATUS
