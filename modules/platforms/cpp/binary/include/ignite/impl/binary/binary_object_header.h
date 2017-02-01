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

/**
 * @file
 * Declares ignite::impl::binary::BinaryObjectHeader class.
 */

#ifndef _IGNITE_IMPL_BINARY_BINARY_OBJECT_HEADER
#define _IGNITE_IMPL_BINARY_BINARY_OBJECT_HEADER

#include <stdint.h>

#include <ignite/common/common.h>

#include <ignite/impl/binary/binary_common.h>
#include <ignite/impl/interop/interop_memory.h>

namespace ignite
{
    namespace impl
    {
        namespace binary
        {

            // This is a packed structure - we do not want padding for our fields here.
#pragma pack(push, 1)

            /**
             * Binary object header layout.
             */
            struct IGNITE_IMPORT_EXPORT BinaryObjectHeaderLayout
            {
                int8_t  headerType;
                int8_t  version;
                int16_t flags;
                int32_t typeId;
                int32_t hashCode;
                int32_t length;
                int32_t schemaId;
                int32_t schemaOffset;
            };

#pragma pack(pop)

            /**
             * Binary object header class.
             *
             * @note Most methods are defined in header to encourage inlining.
             */
            class IGNITE_IMPORT_EXPORT BinaryObjectHeader
            {
            public:
                // Header size in bytes.
                enum { SIZE = sizeof(BinaryObjectHeaderLayout) };

                /**
                 * Create from InteropMemory instance.
                 * @throw IgniteError if the memory at the specified offset
                 *    is not a valid BinaryObject.
                 *
                 * @param mem Memory.
                 * @param offset Offset in memory.
                 * @return New BinaryObjectHeader instance.
                 */
                static BinaryObjectHeader FromMemory(interop::InteropMemory& mem, int32_t offset);

                /**
                 * Constructor.
                 *
                 * @param mem Pointer to header memory.
                 */
                BinaryObjectHeader(void* mem) :
                    header(reinterpret_cast<BinaryObjectHeaderLayout*>(mem))
                {
                    // No-op. 
                }

                /**
                 * Copy constructor.
                 *
                 * @param other Instance to copy.
                 */
                BinaryObjectHeader(const BinaryObjectHeader& other) : 
                    header(other.header)
                {
                    // No-op.
                }

                /**
                 * Assingment operator.
                 *
                 * @param other Other instance.
                 * @return Reference to this.
                 */
                BinaryObjectHeader& operator=(const BinaryObjectHeader& other)
                {
                    header = other.header;

                    return *this;
                }

                /**
                 * Get header type.
                 *
                 * @return Header type.
                 */
                int8_t GetType() const
                {
                    return header->headerType;
                }

                /**
                 * Get version.
                 *
                 * @return Binary object layout version.
                 */
                int8_t GetVersion() const
                {
                    return header->version;
                }

                /**
                 * Get flags.
                 *
                 * @return Flags.
                 */
                int16_t GetFlags() const
                {
                    return header->flags;
                }

                /**
                 * Get type ID.
                 *
                 * @return Type ID.
                 */
                int32_t GetTypeId() const
                {
                    return header->typeId;
                }

                /**
                 * Get hash code.
                 *
                 * @return Hash code.
                 */
                int32_t GetHashCode() const
                {
                    return header->hashCode;
                }

                /**
                 * Get object length.
                 *
                 * @return Object length.
                 */
                int32_t GetLength() const
                {
                    return header->length;
                }

                /**
                 * Get schema ID.
                 *
                 * @return Schema ID.
                 */
                int32_t GetSchemaId() const
                {
                    return header->schemaId;
                }

                /**
                 * Get schema offset.
                 *
                 * @return Schema offset.
                 */
                int32_t GetSchemaOffset() const
                {
                    return header->schemaOffset;
                }

                /**
                 * Check if the binary object has schema.
                 *
                 * @return True if the binary object has schema.
                 */
                bool HasSchema() const
                {
                    return (header->flags & IGNITE_BINARY_FLAG_HAS_SCHEMA) != 0;
                }

                /**
                 * Check if the binary object is of user-defined type.
                 *
                 * @return True if the binary object is of user-defined type.
                 */
                bool IsUserType() const
                {
                    return (header->flags & IGNITE_BINARY_FLAG_USER_TYPE) != 0;
                }

                /**
                 * Get footer offset.
                 *
                 * @return Footer offset.
                 */
                int32_t GetFooterOffset() const
                {
                    // No schema: all we have is data. There is no offset in last 4 bytes.
                    if (!HasSchema())
                        return GetLength();

                    // There is schema. Regardless of raw data presence, footer starts with schema.
                    return GetSchemaOffset();
                }

                /**
                 * Get size of data without header and footer.
                 *
                 * @return Data length.
                 */
                int32_t GetDataLength() const
                {
                    return GetFooterOffset() - SIZE;
                }

            private:
                /** Header layout */
                BinaryObjectHeaderLayout* header;
            };
        }
    }
}

#endif //_IGNITE_IMPL_BINARY_BINARY_OBJECT_HEADER