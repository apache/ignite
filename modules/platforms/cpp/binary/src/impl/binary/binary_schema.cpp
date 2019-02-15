/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

#include <cassert>

#include "ignite/impl/binary/binary_schema.h"
#include "ignite/impl/binary/binary_writer_impl.h"

/** FNV1 hash offset basis. */
enum { FNV1_OFFSET_BASIS = 0x811C9DC5 };

/** FNV1 hash prime. */
enum { FNV1_PRIME = 0x01000193 };

namespace ignite
{
    namespace impl
    {
        namespace binary
        {
            BinarySchema::BinarySchema(): id(0), fieldsInfo(new FieldContainer())
            {
                // No-op.
            }

            BinarySchema::~BinarySchema()
            {
                delete fieldsInfo;
            }

            void BinarySchema::AddField(int32_t fieldId, int32_t offset)
            {
                if (!id)
                {
                    // Initialize offset when the first field is written.
                    id = FNV1_OFFSET_BASIS;
                }

                // Advance schema hash.
                int32_t idAccumulator = id ^ (fieldId & 0xFF);
                idAccumulator *= FNV1_PRIME;
                idAccumulator ^= (fieldId >> 8) & 0xFF;
                idAccumulator *= FNV1_PRIME;
                idAccumulator ^= (fieldId >> 16) & 0xFF;
                idAccumulator *= FNV1_PRIME;
                idAccumulator ^= (fieldId >> 24) & 0xFF;
                idAccumulator *= FNV1_PRIME;

                id = idAccumulator;

                BinarySchemaFieldInfo info = { fieldId, offset };
                fieldsInfo->push_back(info);
            }

            void BinarySchema::Write(interop::InteropOutputStream& out) const
            {
                switch (GetType())
                {
                    case BinaryOffsetType::ONE_BYTE:
                    {
                        for (FieldContainer::const_iterator i = fieldsInfo->begin(); i != fieldsInfo->end(); ++i)
                        {
                            out.WriteInt32(i->id);
                            out.WriteInt8(static_cast<int8_t>(i->offset));
                        }
                        break;
                    }

                    case BinaryOffsetType::TWO_BYTES:
                    {
                        for (FieldContainer::const_iterator i = fieldsInfo->begin(); i != fieldsInfo->end(); ++i)
                        {
                            out.WriteInt32(i->id);
                            out.WriteInt16(static_cast<int16_t>(i->offset));
                        }
                        break;
                    }

                    case BinaryOffsetType::FOUR_BYTES:
                    {
                        for (FieldContainer::const_iterator i = fieldsInfo->begin(); i != fieldsInfo->end(); ++i)
                        {
                            out.WriteInt32(i->id);
                            out.WriteInt32(i->offset);
                        }
                        break;
                    }

                    default:
                    {
                        assert(false);
                        break;
                    }
                }
            }

            bool BinarySchema::Empty() const
            {
                return fieldsInfo->empty();
            }

            void BinarySchema::Clear()
            {
                id = 0;
                fieldsInfo->clear();
            }

            BinaryOffsetType::Type BinarySchema::GetType() const
            {
                int32_t maxOffset = fieldsInfo->back().offset;

                if (maxOffset < 0x100)
                    return BinaryOffsetType::ONE_BYTE;

                if (maxOffset < 0x10000)
                    return BinaryOffsetType::TWO_BYTES;

                return BinaryOffsetType::FOUR_BYTES;
            }
        }
    }
}