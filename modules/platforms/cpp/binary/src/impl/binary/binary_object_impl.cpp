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

#include <ignite/impl/interop/interop_stream_position_guard.h>
#include <ignite/impl/interop/interop_input_stream.h>

#include <ignite/impl/binary/binary_object_header.h>
#include <ignite/impl/binary/binary_object_impl.h>
#include <ignite/impl/binary/binary_utils.h>

using namespace ignite::impl::binary;

namespace ignite
{
    namespace impl
    {
        namespace binary
        {
            BinaryObjectImpl::BinaryObjectImpl(interop::InteropMemory& mem,
                int32_t start, BinaryIdResolver* idRslvr, BinaryTypeManager* metaMgr) :
                mem(&mem),
                start(start),
                idRslvr(0),
                metaMgr(metaMgr),
                binary(false)
            {
                if (idRslvr)
                    this->idRslvr = idRslvr->Clone();

                int8_t hdr = BinaryUtils::ReadInt8(mem, this->start);

                if (hdr == IGNITE_TYPE_BINARY)
                {
                    binary = true;

                    int32_t portLen = BinaryUtils::ReadInt32(mem, this->start + 1);
                    int32_t portOff = BinaryUtils::ReadInt32(mem, this->start + 5 + portLen);

                    this->start += portOff + 5;
                }
                else if (hdr != IGNITE_TYPE_OBJECT)
                {
                    IGNITE_ERROR_FORMATTED_3(ignite::IgniteError::IGNITE_ERR_BINARY,
                        "Memory layuout does not look like a binary object",
                        "memPtr", mem.Data(), "pos", start, "header", hdr);
                }
            }

            BinaryObjectImpl::~BinaryObjectImpl()
            {
                delete idRslvr;
            }

            BinaryObjectImpl::BinaryObjectImpl(const BinaryObjectImpl& other) :
                mem(other.mem),
                start(other.start),
                idRslvr(0),
                metaMgr(other.metaMgr)
            {
                if (other.idRslvr)
                    this->idRslvr = other.idRslvr->Clone();
            }

            BinaryObjectImpl& BinaryObjectImpl::operator=(const BinaryObjectImpl& other)
            {
                if (this != &other)
                {
                    BinaryObjectImpl tmp(other);

                    Swap(tmp);
                }

                return *this;
            }

            void BinaryObjectImpl::Swap(BinaryObjectImpl& other)
            {
                std::swap(mem, other.mem);
                std::swap(start, other.start);
                std::swap(idRslvr, other.idRslvr);
                std::swap(metaMgr, other.metaMgr);
            }

            BinaryObjectImpl BinaryObjectImpl::FromMemory(interop::InteropMemory& mem, int32_t offset,
                BinaryTypeManager* metaMgr)
            {
                BinaryObjectHeader header = BinaryObjectHeader::FromMemory(mem, offset);

                int32_t adjustedStart = static_cast<int32_t>(header.GetMem() - mem.Data());

                assert(adjustedStart >= 0);

                return BinaryObjectImpl(mem, adjustedStart, 0, metaMgr);
            }

            template<>
            IGNITE_IMPORT_EXPORT BinaryObjectImpl BinaryObjectImpl::GetField(const char* name) const
            {
                CheckIdResolver();

                int32_t fieldId = idRslvr->GetFieldId(GetTypeId(), name);
                int32_t pos = FindField(fieldId);

                return FromMemory(*mem, pos, metaMgr);
            }

            bool BinaryObjectImpl::HasField(const char* name) const
            {
                CheckIdResolver();

                int32_t fieldId = idRslvr->GetFieldId(GetTypeId(), name);

                int32_t fieldPos = FindField(fieldId);

                return fieldPos >= 0;
            }

            int32_t BinaryObjectImpl::GetEnumValue() const
            {
                throw IgniteError(IgniteError::IGNITE_ERR_BINARY, "GetEnumValue is only supported for enums.");
            }

            BinaryObjectImpl BinaryObjectImpl::GetField(int32_t idx)
            {
                int32_t offset = start + BinaryObjectHeader::SIZE;

                for (int32_t i = 0; i < idx; ++i)
                {
                    BinaryObjectHeader fieldHeader = BinaryObjectHeader::FromMemory(*mem, offset);

                    offset += fieldHeader.GetLength();
                }

                return FromMemory(*mem, offset, 0);
            }

            const int8_t* BinaryObjectImpl::GetData() const
            {
                return mem->Data() + start + BinaryObjectHeader::SIZE;
            }

            int32_t BinaryObjectImpl::GetLength() const
            {
                BinaryObjectHeader header(mem->Data() + start);

                return header.GetDataLength();
            }

            int32_t BinaryObjectImpl::GetTypeId() const
            {
                BinaryObjectHeader header(mem->Data() + start);

                return header.GetTypeId();
            }

            int32_t BinaryObjectImpl::GetHashCode() const
            {
                BinaryObjectHeader header(mem->Data() + start);

                return header.GetHashCode();
            }

            int32_t BinaryObjectImpl::FindField(const int32_t fieldId) const
            {
                BinaryObjectHeader header(mem->Data() + start);
                int32_t flags = header.GetFlags();

                int32_t footerBegin = header.GetFooterOffset() + start;
                int32_t footerEnd = footerBegin + header.GetFooterLength();

                if ((mem->Length() - start) < footerEnd)
                {
                    IGNITE_ERROR_FORMATTED_3(ignite::IgniteError::IGNITE_ERR_BINARY,
                        "Not enough data in the binary object", "memPtr", mem->PointerLong(),
                        "len", (mem->Length() - start), "footerEnd", footerEnd);
                }

                if (flags & IGNITE_BINARY_FLAG_OFFSET_ONE_BYTE)
                {
                    for (int32_t schemaPos = footerBegin; schemaPos < footerEnd; schemaPos += 5)
                    {
                        int32_t currentFieldId = BinaryUtils::UnsafeReadInt32(*mem, schemaPos);

                        if (fieldId == currentFieldId)
                            return (BinaryUtils::UnsafeReadInt8(*mem, schemaPos + 4) & 0xFF) + start;
                    }
                }
                else if (flags & IGNITE_BINARY_FLAG_OFFSET_TWO_BYTES)
                {
                    for (int32_t schemaPos = footerBegin; schemaPos < footerEnd; schemaPos += 6)
                    {
                        int32_t currentFieldId = BinaryUtils::UnsafeReadInt32(*mem, schemaPos);

                        if (fieldId == currentFieldId)
                            return (BinaryUtils::UnsafeReadInt16(*mem, schemaPos + 4) & 0xFFFF) + start;
                    }
                }
                else
                {
                    for (int32_t schemaPos = footerBegin; schemaPos < footerEnd; schemaPos += 8)
                    {
                        int32_t currentFieldId = BinaryUtils::UnsafeReadInt32(*mem, schemaPos);

                        if (fieldId == currentFieldId)
                            return BinaryUtils::UnsafeReadInt32(*mem, schemaPos + 4) + start;
                    }
                }

                return -1;
            }

            void BinaryObjectImpl::CheckIdResolver() const
            {
                if (idRslvr)
                    return;

                assert(metaMgr != 0);

                BinaryObjectHeader header(mem->Data() + start);

                int32_t typeId = header.GetTypeId();

                SPSnap meta = metaMgr->GetMeta(typeId);

                idRslvr = new MetadataBinaryIdResolver(meta);
            }
        }
    }
}
