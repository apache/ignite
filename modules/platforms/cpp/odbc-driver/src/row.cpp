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

#include "utility.h"

#include <ignite/impl/interop/interop_stream_position_guard.h>

#include "row.h"

namespace ignite
{
    namespace odbc
    {
        Row::Row(ignite::impl::interop::InteropUnpooledMemory& pageData) :
            pageData(pageData), stream(&pageData)
        {
            size = stream.ReadInt32();

            rowBeginPos = stream.Position();
        }

        Row::~Row()
        {
            // No-op.
        }

        void Row::ReadColumnToBuffer(ApplicationDataBuffer& dataBuf)
        {
            using namespace ignite::impl::binary;
            using namespace ignite::impl::interop;

            BinaryReaderImpl reader(&stream);

            int8_t hdr;

            // Reading header without changing stream position.
            {
                InteropStreamPositionGuard<InteropInputStream> guard(stream);

                hdr = stream.ReadInt8();
            }
            
            int32_t objectSize = 0;

            switch (hdr)
            {
                case IGNITE_TYPE_BYTE:
                {
                    dataBuf.PutInt8(reader.ReadInt8());
                    break;
                }

                case IGNITE_TYPE_SHORT:
                case IGNITE_TYPE_CHAR:
                {
                    dataBuf.PutInt16(reader.ReadInt16());
                    break;
                }

                case IGNITE_TYPE_INT:
                {
                    dataBuf.PutInt32(reader.ReadInt32());
                    break;
                }

                case IGNITE_TYPE_LONG:
                {
                    dataBuf.PutInt64(reader.ReadInt64());
                    break;
                }

                case IGNITE_TYPE_FLOAT:
                {
                    dataBuf.PutFloat(reader.ReadFloat());
                    break;
                }

                case IGNITE_TYPE_DOUBLE:
                {
                    dataBuf.PutDouble(reader.ReadDouble());
                    break;
                }

                case IGNITE_TYPE_BOOL:
                {
                    dataBuf.PutInt8(reader.ReadBool() ? 1 : 0);
                    break;
                }

                case IGNITE_TYPE_STRING:
                {
                    std::string str;
                    utility::ReadString(reader, str);

                    dataBuf.PutString(str);
                    break;
                }

                case IGNITE_TYPE_UUID:
                {
                    Guid guid = reader.ReadGuid();

                    dataBuf.PutGuid(guid);
                    break;
                }

                case IGNITE_TYPE_DECIMAL:
                case IGNITE_TYPE_DATE:
                default:
                {
                    // No-op.
                    break;
                }
            }
        }
    }
}

