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

#include "application_data_buffer.h"

namespace ignite
{
    namespace odbc
    {
        ApplicationDataBuffer::ApplicationDataBuffer() :
            type(0), buffer(0), buflen(0), reslen(0)
        {
            // No-op.
        }

        ApplicationDataBuffer::ApplicationDataBuffer(uint16_t type, void* bufferPtr, int64_t buflen, int64_t* reslen) :
            type(type), buffer(bufferPtr), buflen(buflen), reslen(reslen)
        {
            // No-op.
        }

        ApplicationDataBuffer::ApplicationDataBuffer(const ApplicationDataBuffer & other) :
            type(other.type), buffer(other.buffer), buflen(other.buflen), reslen(other.reslen)
        {
            // No-op.
        }

        ApplicationDataBuffer::~ApplicationDataBuffer()
        {
            // No-op.
        }

        ApplicationDataBuffer & ApplicationDataBuffer::operator=(const ApplicationDataBuffer & other)
        {
            type = other.type;
            buffer = other.buffer;
            buflen = other.buflen;
            reslen = other.reslen;

            return *this;
        }
    }
}

