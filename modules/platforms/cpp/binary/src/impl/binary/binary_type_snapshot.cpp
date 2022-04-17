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
            IGNITE_IMPORT_EXPORT BinaryTypeSnapshot::BinaryTypeSnapshot(const std::string& typeName,
                const std::string& affFieldName, int32_t typeId) :
                typeName(typeName),
                affFieldName(affFieldName),
                typeId(typeId),
                fieldIds(),
                fields()
            {
                // No-op.
            }

            BinaryTypeSnapshot::BinaryTypeSnapshot(const BinaryTypeSnapshot& another) :
                typeName(another.typeName),
                affFieldName(another.affFieldName),
                typeId(another.typeId),
                fieldIds(another.fieldIds),
                fields(another.fields)
            {
                // No-op.
            }

            IGNITE_IMPORT_EXPORT void BinaryTypeSnapshot::AddField(int32_t fieldId, const std::string& fieldName, int32_t fieldTypeId)
            {
                fieldIds.insert(fieldId);
                fields[fieldName] = BinaryFieldMeta(fieldTypeId, fieldId);
            }

            void BinaryTypeSnapshot::CopyFieldsFrom(const BinaryTypeSnapshot* another)
            {
                if (another && another->HasFields())
                {
                    fieldIds.insert(another->fieldIds.begin(), another->fieldIds.end());
                    fields.insert(another->fields.begin(), another->fields.end());
                }
            }
        }
    }
}
