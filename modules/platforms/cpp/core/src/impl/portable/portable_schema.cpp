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

            void PortableSchema::AddField(int32_t id, int32_t offset)
            {
                PortableSchemaFieldInfo info = { id, offset };
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