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

#include <cassert>

#include <ignite/impl/binary/binary_utils.h>

#include "ignite/odbc/utility.h"
#include "ignite/odbc/system/odbc_constants.h"

namespace ignite
{
    namespace utility
    {
        size_t CopyStringToBuffer(const std::string& str, char* buf, size_t buflen)
        {
            if (!buf || !buflen)
                return 0;

            size_t bytesToCopy = std::min(str.size(), static_cast<size_t>(buflen - 1));

            memcpy(buf, str.data(), bytesToCopy);
            buf[bytesToCopy] = 0;

            return bytesToCopy;
        }

        void ReadString(ignite::impl::binary::BinaryReaderImpl& reader, std::string& str)
        {
            int32_t strLen = reader.ReadString(0, 0);

            if (strLen > 0)
            {
                str.resize(strLen);

                reader.ReadString(&str[0], static_cast<int32_t>(str.size()));
            }
            else
            {
                str.clear();

                if (strLen == 0)
                {
                    char dummy;

                    reader.ReadString(&dummy, sizeof(dummy));
                }
            }
        }

        void WriteString(ignite::impl::binary::BinaryWriterImpl& writer, const std::string & str)
        {
            writer.WriteString(str.data(), static_cast<int32_t>(str.size()));
        }

        void ReadDecimal(ignite::impl::binary::BinaryReaderImpl& reader, common::Decimal& decimal)
        {
            int8_t hdr = reader.ReadInt8();

            assert(hdr == ignite::impl::binary::IGNITE_TYPE_DECIMAL);

            IGNITE_UNUSED(hdr);

            int32_t scale = reader.ReadInt32();

            int32_t len = reader.ReadInt32();

            std::vector<int8_t> mag;

            mag.resize(len);

            impl::binary::BinaryUtils::ReadInt8Array(reader.GetStream(), mag.data(), static_cast<int32_t>(mag.size()));

            int32_t sign = 1;
            
            if (mag[0] < 0)
            {
                mag[0] &= 0x7F;

                sign = -1;
            }

            common::Decimal res(mag.data(), static_cast<int32_t>(mag.size()), scale, sign);

            decimal.Swap(res);
        }

        void WriteDecimal(ignite::impl::binary::BinaryWriterImpl& writer, const common::Decimal& decimal)
        {
            writer.WriteInt8(ignite::impl::binary::IGNITE_TYPE_DECIMAL);

            const common::BigInteger &unscaled = decimal.GetUnscaledValue();

            writer.WriteInt32(decimal.GetScale());

            common::FixedSizeArray<int8_t> magnitude;

            unscaled.MagnitudeToBytes(magnitude);

            int8_t addBit = unscaled.GetSign() == -1 ? -0x80 : 0;

            if (magnitude[0] < 0)
            {
                writer.WriteInt32(magnitude.GetSize() + 1);
                writer.WriteInt8(addBit);
            }
            else
            {
                writer.WriteInt32(magnitude.GetSize());
                magnitude[0] |= addBit;
            }

            impl::binary::BinaryUtils::WriteInt8Array(writer.GetStream(), magnitude.GetData(), magnitude.GetSize());
        }

        std::string SqlStringToString(const unsigned char* sqlStr, int32_t sqlStrLen)
        {
            std::string res;

            const char* sqlStrC = reinterpret_cast<const char*>(sqlStr);

            if (!sqlStr || !sqlStrLen)
                return res;

            if (sqlStrLen == SQL_NTS)
                res.assign(sqlStrC);
            else if (sqlStrLen > 0)
                res.assign(sqlStrC, sqlStrLen);

            return res;
        }

        void ReadByteArray(impl::binary::BinaryReaderImpl& reader, std::vector<int8_t>& res)
        {
            int32_t len = reader.ReadInt8Array(0, 0);

            if (len > 0)
            {
                res.resize(len);

                reader.ReadInt8Array(&res[0], static_cast<int32_t>(res.size()));
            }
            else
                res.clear();
        }

        std::string HexDump(const void* data, size_t count)
        {
            std::stringstream  dump;
            size_t cnt = 0;
            for(const uint8_t* p = (const uint8_t*)data, *e = (const uint8_t*)data + count; p != e; ++p)
            {
                if (cnt++ % 16 == 0)
                {
                    dump << std::endl;
                }
                dump << std::hex << std::setfill('0') << std::setw(2) << (int)*p << " ";
            }
            return dump.str();
        }
    }
}

