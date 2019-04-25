/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
                fields()
            {
                // No-op.
            }

            BinaryTypeSnapshot::BinaryTypeSnapshot(const BinaryTypeSnapshot& another) :
                typeName(another.typeName),
                typeId(another.typeId),
                fieldIds(another.fieldIds),
                fields(another.fields)
            {
                // No-op.
            }

            void BinaryTypeSnapshot::AddField(int32_t fieldId, const std::string& fieldName, int32_t fieldTypeId)
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