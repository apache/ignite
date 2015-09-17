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

#include "ignite/impl/portable/portable_metadata_snapshot.h"

namespace ignite
{    
    namespace impl
    {
        namespace portable
        {
            PortableMetadataSnapshot::PortableMetadataSnapshot(std::string typeName, int32_t typeId, 
                std::set<int32_t>* fieldIds, std::map<std::string, int32_t>* fields) : 
                typeName(typeName), typeId(typeId), fieldIds(fieldIds), fields(fields)
            {
                // No-op.
            }

            PortableMetadataSnapshot::~PortableMetadataSnapshot()
            {
                delete fieldIds;
                delete fields;
            }

            bool PortableMetadataSnapshot::ContainsFieldId(int32_t fieldId)
            {
                return fieldIds && fieldIds->count(fieldId) == 1;
            }

            std::string PortableMetadataSnapshot::GetTypeName()
            {
                return typeName;
            }

            int32_t PortableMetadataSnapshot::GetTypeId()
            {
                return typeId;
            }

            bool PortableMetadataSnapshot::HasFields()
            {
                return !fieldIds->empty();
            }

            std::set<int32_t>* PortableMetadataSnapshot::GetFieldIds()
            {
                return fieldIds;
            }

            std::map<std::string, int32_t>* PortableMetadataSnapshot::GetFields()
            {
                return fields;
            }
        }
    }
}