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

#include "ignite/impl/portable/portable_metadata_handler.h"

using namespace ignite::common::concurrent;

namespace ignite
{    
    namespace impl
    {
        namespace portable
        {
            PortableMetadataHandler::PortableMetadataHandler(SPSnap snap) : snap(snap), fieldIds(NULL), fields(NULL)
            {
                // No-op.
            }
            
            PortableMetadataHandler::~PortableMetadataHandler()
            {
                if (fieldIds)
                    delete fieldIds;

                if (fields)
                    delete fields;
            }

            void PortableMetadataHandler::OnFieldWritten(int32_t fieldId, std::string fieldName, int32_t fieldTypeId)
            {
                if (!snap.Get() || !snap.Get()->ContainsFieldId(fieldId))
                {
                    if (!HasDifference())
                    {
                        fieldIds = new std::set<int32_t>();
                        fields = new std::map<std::string, int32_t>();
                    }

                    fieldIds->insert(fieldId);
                    (*fields)[fieldName] = fieldTypeId;
                }
            }

            SPSnap PortableMetadataHandler::GetSnapshot()
            {
                return snap;
            }

            bool PortableMetadataHandler::HasDifference()
            {
                return fieldIds ? true : false;
            }

            std::set<int32_t>* PortableMetadataHandler::GetFieldIds()
            {
                return fieldIds;
            }

            std::map<std::string, int32_t>* PortableMetadataHandler::GetFields()
            {
                return fields;
            }
        }
    }
}