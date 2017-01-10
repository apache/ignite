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

#include <ignite/impl/interop/interop_stream_position_guard.h>

#include "ignite/odbc/utility.h"
#include "ignite/odbc/column.h"

namespace
{
    using namespace ignite::impl::interop;
    using namespace ignite::impl::binary;

    bool GetObjectLength(InteropInputStream& stream, int32_t& len)
    {
        InteropStreamPositionGuard<InteropInputStream> guard(stream);

        int8_t hdr = stream.ReadInt8();

        switch (hdr)
        {
            case IGNITE_TYPE_BINARY:
            {
                // Header field + Length field + Object itself + Offset field
                len = 1 + 4 + stream.ReadInt32() + 4;

                break;
            }

            case IGNITE_TYPE_OBJECT:
            {
                int8_t protoVer = stream.ReadInt8();

                if (protoVer != IGNITE_PROTO_VER)
                    return false;

                // Skipping flags, typeId and hash code
                len = stream.ReadInt32(stream.Position() + 2 + 4 + 4);

                break;
            }

            default:
                return false;
        }

        return true;
    }

    /**
     * Read column header and restores position if the column is of
     * complex type.
     * @return Column type header.
     */
    int8_t ReadColumnHeader(InteropInputStream& stream)
    {
        using namespace ignite::impl::binary;

        int32_t headerPos = stream.Position();

        int8_t hdr = stream.ReadInt8();

        // Check if we need to restore position - to read complex types
        // stream should have unread header, but for primitive types it
        // should not.
        switch (hdr)
        {
            case IGNITE_TYPE_BYTE:
            case IGNITE_TYPE_SHORT:
            case IGNITE_TYPE_CHAR:
            case IGNITE_TYPE_INT:
            case IGNITE_TYPE_LONG:
            case IGNITE_TYPE_FLOAT:
            case IGNITE_TYPE_DOUBLE:
            case IGNITE_TYPE_BOOL:
            case IGNITE_HDR_NULL:
            case IGNITE_TYPE_ARRAY_BYTE:
            {
                // No-op.
                break;
            }

            default:
            {
                // Restoring position.
                stream.Position(headerPos);
                break;
            }
        }

        return hdr;
    }
}

namespace ignite
{
    namespace odbc
    {
        Column::Column() :
            type(0), startPos(-1), endPos(-1), offset(0), size(0)
        {
            // No-op.
        }

        Column::Column(const Column& other) :
            type(other.type), startPos(other.startPos), endPos(other.endPos),
            offset(other.offset), size(other.size)
        {
            // No-op.
        }

        Column& Column::operator=(const Column& other)
        {
            type = other.type;
            startPos = other.startPos;
            endPos = other.endPos;
            offset = other.offset;
            size = other.size;

            return *this;
        }

        Column::~Column()
        {
            // No-op.
        }

        Column::Column(BinaryReaderImpl& reader) :
            type(0), startPos(-1), endPos(-1), offset(0), size(0)
        {
            InteropInputStream* stream = reader.GetStream();

            if (!stream)
                return;

            InteropStreamPositionGuard<InteropInputStream> guard(*stream);

            int32_t sizeTmp = 0;

            int8_t hdr = ReadColumnHeader(*stream);

            int32_t startPosTmp = stream->Position();

            switch (hdr)
            {
                case IGNITE_HDR_NULL:
                {
                    sizeTmp = 1;

                    break;
                }

                case IGNITE_TYPE_BYTE:
                {
                    reader.ReadInt8();

                    sizeTmp = 1;

                    break;
                }

                case IGNITE_TYPE_BOOL:
                {
                    reader.ReadBool();

                    sizeTmp = 1;

                    break;
                }

                case IGNITE_TYPE_SHORT:
                case IGNITE_TYPE_CHAR:
                {
                    reader.ReadInt16();

                    sizeTmp = 2;

                    break;
                }

                case IGNITE_TYPE_FLOAT:
                {
                    reader.ReadFloat();

                    sizeTmp = 4;

                    break;
                }

                case IGNITE_TYPE_INT:
                {
                    reader.ReadInt32();

                    sizeTmp = 4;

                    break;
                }

                case IGNITE_TYPE_DOUBLE:
                {
                    reader.ReadDouble();

                    sizeTmp = 8;

                    break;
                }

                case IGNITE_TYPE_LONG:
                {
                    reader.ReadInt64();

                    sizeTmp = 8;

                    break;
                }

                case IGNITE_TYPE_STRING:
                {
                    std::string str;
                    utility::ReadString(reader, str);

                    sizeTmp = static_cast<int32_t>(str.size());

                    break;
                }

                case IGNITE_TYPE_UUID:
                {
                    reader.ReadGuid();

                    sizeTmp = 16;

                    break;
                }

                case IGNITE_TYPE_BINARY:
                case IGNITE_TYPE_OBJECT:
                {
                    int32_t len;

                    if (!GetObjectLength(*stream, len))
                        return;

                    sizeTmp = len;

                    stream->Position(stream->Position() + len);

                    break;
                }

                case IGNITE_TYPE_DECIMAL:
                {
                    common::Decimal res;

                    utility::ReadDecimal(reader, res);

                    sizeTmp = res.GetMagnitudeLength() + 8;

                    break;
                }

                case IGNITE_TYPE_DATE:
                {
                    reader.ReadDate();

                    sizeTmp = 8;

                    break;
                }

                case IGNITE_TYPE_TIMESTAMP:
                {
                    reader.ReadTimestamp();

                    sizeTmp = 12;

                    break;
                }

                case IGNITE_TYPE_ARRAY_BYTE:
                {
                    sizeTmp = reader.ReadInt32();
                    assert(sizeTmp >= 0);

                    startPosTmp = stream->Position();
                    stream->Position(stream->Position() + sizeTmp);

                    break;
                }

                default:
                {
                    // This is a fail case.
                    assert(false);
                    return;
                }
            }

            type = hdr;
            startPos = startPosTmp;
            endPos = stream->Position();
            size = sizeTmp;
        }

        SqlResult Column::ReadToBuffer(BinaryReaderImpl& reader, app::ApplicationDataBuffer& dataBuf)
        {
            if (!IsValid())
                return SQL_RESULT_ERROR;

            if (GetUnreadDataLength() == 0)
            {
                dataBuf.PutNull();

                return SQL_RESULT_NO_DATA;
            }

            InteropInputStream* stream = reader.GetStream();

            if (!stream)
                return SQL_RESULT_ERROR;

            InteropStreamPositionGuard<InteropInputStream> guard(*stream);

            stream->Position(startPos);

            switch (type)
            {
                case IGNITE_TYPE_BYTE:
                {
                    dataBuf.PutInt8(reader.ReadInt8());

                    IncreaseOffset(size);

                    break;
                }

                case IGNITE_TYPE_SHORT:
                case IGNITE_TYPE_CHAR:
                {
                    dataBuf.PutInt16(reader.ReadInt16());

                    IncreaseOffset(size);

                    break;
                }

                case IGNITE_TYPE_INT:
                {
                    dataBuf.PutInt32(reader.ReadInt32());

                    IncreaseOffset(size);

                    break;
                }

                case IGNITE_TYPE_LONG:
                {
                    dataBuf.PutInt64(reader.ReadInt64());

                    IncreaseOffset(size);

                    break;
                }

                case IGNITE_TYPE_FLOAT:
                {
                    dataBuf.PutFloat(reader.ReadFloat());

                    IncreaseOffset(size);

                    break;
                }

                case IGNITE_TYPE_DOUBLE:
                {
                    dataBuf.PutDouble(reader.ReadDouble());

                    IncreaseOffset(size);

                    break;
                }

                case IGNITE_TYPE_BOOL:
                {
                    dataBuf.PutInt8(reader.ReadBool() ? 1 : 0);

                    IncreaseOffset(size);

                    break;
                }

                case IGNITE_TYPE_STRING:
                {
                    std::string str;
                    utility::ReadString(reader, str);

                    int32_t written = dataBuf.PutString(str.substr(offset));

                    IncreaseOffset(written);

                    break;
                }

                case IGNITE_TYPE_UUID:
                {
                    Guid guid = reader.ReadGuid();

                    dataBuf.PutGuid(guid);

                    IncreaseOffset(size);

                    break;
                }

                case IGNITE_HDR_NULL:
                {
                    dataBuf.PutNull();

                    IncreaseOffset(size);

                    break;
                }

                case IGNITE_TYPE_BINARY:
                case IGNITE_TYPE_OBJECT:
                {
                    int32_t len;

                    if (!GetObjectLength(*stream, len))
                        return SQL_RESULT_ERROR;

                    std::vector<int8_t> data(len);

                    stream->ReadInt8Array(&data[0], static_cast<int32_t>(data.size()));

                    int32_t written = dataBuf.PutBinaryData(data.data() + offset, static_cast<size_t>(len - offset));

                    IncreaseOffset(written);

                    break;
                }

                case IGNITE_TYPE_DECIMAL:
                {
                    common::Decimal res;

                    utility::ReadDecimal(reader, res);

                    dataBuf.PutDecimal(res);

                    IncreaseOffset(size);

                    break;
                }

                case IGNITE_TYPE_DATE:
                {
                    Date date = reader.ReadDate();

                    dataBuf.PutDate(date);

                    break;
                }

                case IGNITE_TYPE_TIMESTAMP:
                {
                    Timestamp ts = reader.ReadTimestamp();

                    dataBuf.PutTimestamp(ts);

                    break;
                }

                case IGNITE_TYPE_ARRAY_BYTE:
                {
                    stream->Position(startPos + offset);
                    int32_t maxRead = std::min(GetUnreadDataLength(), static_cast<int32_t>(dataBuf.GetSize()));
                    std::vector<int8_t> data(maxRead);

                    stream->ReadInt8Array(&data[0], static_cast<int32_t>(data.size()));

                    int32_t written = dataBuf.PutBinaryData(data.data(), data.size());

                    IncreaseOffset(written);
                    break;
                }

                default:
                {
                    // This is a fail case. Return false.
                    return SQL_RESULT_ERROR;
                }
            }

            return SQL_RESULT_SUCCESS;
        }

        void Column::IncreaseOffset(int32_t value)
        {
            offset += value;

            if (offset > size)
                offset = size;
        }
    }
}

