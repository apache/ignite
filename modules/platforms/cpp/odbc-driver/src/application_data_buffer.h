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

#ifndef _IGNITE_ODBC_DRIVER_APPLICATION_DATA_BUFFER
#define _IGNITE_ODBC_DRIVER_APPLICATION_DATA_BUFFER

#include <stdint.h>

namespace ignite
{
    namespace odbc
    {
        /**
         * User application data buffer.
         */
        class ApplicationDataBuffer
        {
        public:
            /**
             * Default constructor.
             */
            ApplicationDataBuffer();

            /**
             * Constructor.
             *
             * @param type Underlying data type.
             * @param bufferPtr Data buffer pointer.
             * @param len Data buffer length.
             */
            ApplicationDataBuffer(uint16_t type, void* bufferPtr, uint64_t len);

            /**
             * Copy constructor.
             *
             * @param other Other instance.
             */
            ApplicationDataBuffer(const ApplicationDataBuffer& other);

            /**
             * Destructor.
             */
            ~ApplicationDataBuffer();
            
            /**
             * Copy assigment operator.
             *
             * @param other Other instance.
             * @return This.
             */
            ApplicationDataBuffer& operator=(const ApplicationDataBuffer& other);

        private:
            /** Underlying data type. */
            uint16_t    type;

            /** Buffer pointer. */
            void*       buffer;

            /** Buffer length. */
            uint64_t    len;
        };
    }
}

#endif