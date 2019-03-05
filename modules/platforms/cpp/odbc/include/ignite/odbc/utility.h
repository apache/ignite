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

#ifndef _IGNITE_ODBC_UTILITY
#define _IGNITE_ODBC_UTILITY

#ifdef min
#   undef min
#endif //min

#include <stdint.h>

#include <string>

#include <ignite/common/utils.h>
#include <ignite/common/decimal.h>

#include "ignite/impl/binary/binary_reader_impl.h"
#include "ignite/impl/binary/binary_writer_impl.h"

namespace ignite
{
    namespace utility
    {
        /** Using common version of the util. */
        using common::IntoLower;

        template<typename T>
        T* GetPointerWithOffset(T* ptr, size_t offset)
        {
            uint8_t* ptrBytes = (uint8_t*)ptr;

            return (T*)(ptrBytes + offset);
        }

        /**
         * Copy string to buffer of the specific length.
         * @param str String to copy data from.
         * @param buf Buffer to copy data to.
         * @param buflen Length of the buffer.
         * @return Length of the resulting string in buffer.
         */
        size_t CopyStringToBuffer(const std::string& str, char* buf, size_t buflen);

        /**
         * Read array from reader.
         * @param reader Reader.
         * @param res Resulting vector.
         */
        void ReadByteArray(impl::binary::BinaryReaderImpl& reader, std::vector<int8_t>& res);

        /**
         * Read string from reader.
         * @param reader Reader.
         * @param str String.
         */
        void ReadString(impl::binary::BinaryReaderImpl& reader, std::string& str);

        /**
         * Write string using writer.
         * @param writer Writer.
         * @param str String.
         */
        void WriteString(impl::binary::BinaryWriterImpl& writer, const std::string& str);

        /**
         * Read decimal value using reader.
         *
         * @param reader Reader.
         * @param decimal Decimal value.
         */
        void ReadDecimal(impl::binary::BinaryReaderImpl& reader, common::Decimal& decimal);

        /**
         * Write decimal value using writer.
         *
         * @param writer Writer.
         * @param decimal Decimal value.
         */
        void WriteDecimal(impl::binary::BinaryWriterImpl& writer, const common::Decimal& decimal);

        /**
         * Convert SQL string buffer to std::string.
         *
         * @param sqlStr SQL string buffer.
         * @param sqlStrLen SQL string length.
         * @return Standard string containing the same data.
         */
        std::string SqlStringToString(const unsigned char* sqlStr, int32_t sqlStrLen);

        /**
         * Convert binary data to hex dump form
         * @param data  pointer to data
         * @param count data length
         * @return standard string containing the formated hex dump
         */
        std::string HexDump(const void* data, size_t count);
    }
}

#endif //_IGNITE_ODBC_UTILITY
