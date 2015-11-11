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
            type(0), buffer(0), len(0)
        {
            // No-op.
        }

        ApplicationDataBuffer::ApplicationDataBuffer(uint16_t type, void* bufferPtr, uint64_t len) :
            type(type), buffer(bufferPtr), len(len)
        {
            // No-op.
        }

        ApplicationDataBuffer::ApplicationDataBuffer(const ApplicationDataBuffer & other) :
            type(other.type), buffer(other.buffer), len(other.len)
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
            len = other.len;

            return *this;
        }
    }
}

