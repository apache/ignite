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

#include "connection.h"

#include <ignite/impl/interop/interop_output_stream.h>
#include <ignite/impl/interop/interop_input_stream.h>
#include <ignite/impl/binary/binary_writer_impl.h>

#include "utility.h"
#include "statement.h"

// Temporary solution
#define DEFAULT_PAGE_SIZE 1024

namespace ignite
{
    namespace odbc
    {
        Statement::Statement(Connection& parent) :
            connection(parent), columnBindings(), resultQueryId(0)
        {
            // No-op.
        }

        Statement::~Statement()
        {
            // No-op.
        }

        void Statement::BindResultColumn(uint16_t columnIdx, const ApplicationDataBuffer& buffer)
        {
            columnBindings[columnIdx] = buffer;
        }

        bool Statement::ExecuteSqlQuery(const char* query, size_t len)
        {
            using namespace ignite::impl::interop;

            InteropUnpooledMemory outMem(1024);
            InteropOutputStream outStream(&outMem);

            ignite::impl::binary::BinaryWriterImpl writer(&outStream, 0);

            const std::string& cacheName = connection.GetCache();

            LOG_MSG("Cache name: %s\n", cacheName.c_str());

            writer.WriteString(cacheName.c_str(), static_cast<int32_t>(cacheName.size()));
            writer.WriteString(query, static_cast<int32_t>(len));
            writer.WriteInt32(DEFAULT_PAGE_SIZE);

            writer.WriteInt32(0);

            bool sent = connection.Send(reinterpret_cast<uint8_t*>(outMem.Data()), outStream.Position());

            if (!sent)
                return false;

            std::vector<uint8_t> response;

            bool responseReceived = connection.Receive(response);

            if (!responseReceived)
                return false;

            LOG_MSG("Received response %d bytes long\n", response.size());

            InteropUnpooledMemory inMem(static_cast<int32_t>(response.size()));

            // TODO: optimize me.
            memcpy(inMem.Data(), response.data(), response.size());
            inMem.Length(static_cast<int32_t>(response.size()));

            InteropInputStream inStream(&inMem);

            resultQueryId = inStream.ReadInt64();

            LOG_MSG("Query id: %lld\n", resultQueryId);

            return true;
        }

        SqlResult Statement::FetchRow()
        {
            //
            return SQL_RESULT_NO_DATA;
        }
    }
}

