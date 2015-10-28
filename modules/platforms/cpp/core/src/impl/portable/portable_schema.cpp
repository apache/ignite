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

#include "ignite/impl/portable/portable_schema.h"
#include "ignite/impl/portable/portable_writer_impl.h"

/** FNV1 hash offset basis. */
enum { FNV1_OFFSET_BASIS = 0x811C9DC5 };

/** FNV1 hash prime. */
enum { FNV1_PRIME = 0x01000193 };

namespace ignite
{
    namespace impl
    {
        namespace portable
        {
            PortableSchema::PortableSchema(): id(0), fieldsInfo(new FieldContainer())
            {
                // No-op.
            }

            PortableSchema::~PortableSchema()
            {
                delete fieldsInfo;
            }

            void PortableSchema::AddField(int32_t fieldId, int32_t offset)
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

                PortableSchemaFieldInfo info = { fieldId, offset };
                fieldsInfo->push_back(info);
            }

            void PortableSchema::Write(interop::InteropOutputStream& out) const
            {
                for (FieldContainer::const_iterator i = fieldsInfo->begin(); i != fieldsInfo->end(); ++i)
                {
                    out.WriteInt32(i->id);
                    out.WriteInt32(i->offset);
                }
            }

            bool PortableSchema::Empty() const
            {
                return fieldsInfo->empty();
            }

            void PortableSchema::Clear()
            {
                id = 0;
                fieldsInfo->clear();
            }
        }
    }
}