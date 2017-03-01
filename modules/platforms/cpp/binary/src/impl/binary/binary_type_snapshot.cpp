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

#include "ignite/impl/binary/binary_type_snapshot.h"

namespace ignite
{    
    namespace impl
    {
        namespace binary
        {
            BinaryTypeSnapshot::BinaryTypeSnapshot(std::string typeName, int32_t typeId) :
                typeName(typeName),
                typeId(typeId),
                fieldIds(),
                fieldIdMap(),
                fieldTypeMap()
            {
                // No-op.
            }

            BinaryTypeSnapshot::BinaryTypeSnapshot(const BinaryTypeSnapshot& another) :
                typeName(another.typeName),
                typeId(another.typeId),
                fieldIds(another.fieldIds),
                fieldIdMap(another.fieldIdMap),
                fieldTypeMap(another.fieldTypeMap)
            {
                // No-op.
            }

            void BinaryTypeSnapshot::AddField(int32_t fieldId, const std::string& fieldName, int32_t fieldTypeId)
            {
                fieldIds.insert(fieldId);
                fieldIdMap[fieldName] = fieldId;
                fieldTypeMap[fieldName] = fieldTypeId;
            }

            void BinaryTypeSnapshot::CopyFieldsFrom(const BinaryTypeSnapshot* another)
            {
                if (another && another->HasFields())
                {
                    fieldIds.insert(another->fieldIds.begin(), another->fieldIds.end());
                    fieldIdMap.insert(another->fieldIdMap.begin(), another->fieldIdMap.end());
                    fieldTypeMap.insert(another->fieldTypeMap.begin(), another->fieldTypeMap.end());
                }
            }
        }
    }
}