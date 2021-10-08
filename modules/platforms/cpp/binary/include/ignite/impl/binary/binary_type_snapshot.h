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

#ifndef _IGNITE_IMPL_BINARY_BINARY_TYPE_SNAPSHOT
#define _IGNITE_IMPL_BINARY_BINARY_TYPE_SNAPSHOT

#include <map>
#include <set>
#include <stdint.h>
#include <string>

#include <ignite/common/common.h>
#include <ignite/common/concurrent.h>
#include <ignite/impl/binary/binary_field_meta.h>

namespace ignite
{
    namespace impl
    {
        namespace binary
        {
            /**
             * Type snapshot.
             */
            class BinaryTypeSnapshot
            {
            public:
                typedef std::map<std::string, BinaryFieldMeta> FieldMap;
                typedef std::set<int32_t> FieldIdSet;

                /**
                 * Constructor.
                 *
                 * @param typeName Type name.
                 * @param affFieldName Affinity fiend name.
                 * @param typeId Type ID.
                 */
                IGNITE_IMPORT_EXPORT BinaryTypeSnapshot(const std::string& typeName, const std::string& affFieldName,
                    int32_t typeId);

                /**
                 * Copy constructor.
                 *
                 * @param another Another instance.
                 */
                BinaryTypeSnapshot(const BinaryTypeSnapshot& another);

                /**
                 * Check whether snapshot contains a field with the given ID.
                 *
                 * @param fieldId Field ID.
                 * @return True if contains, false otherwise.
                 */
                bool ContainsFieldId(int32_t fieldId) const
                {
                    return fieldIds.count(fieldId) == 1;
                }

                /**
                 * Get type name.
                 *
                 * @return Type name.
                 */
                const std::string& GetTypeName() const
                {
                    return typeName;
                }

                /**
                 * Get affinity field name.
                 *
                 * @return Affinity field name.
                 */
                const std::string& GetAffinityFieldName() const
                {
                    return affFieldName;
                }

                /**
                 * Get type ID.
                 *
                 * @return Type ID.
                 */
                int32_t GetTypeId() const
                {
                    return typeId;
                }

                /**
                 * Whether snapshot contains any fields.
                 *
                 * @return True if fields exist.
                 */
                bool HasFields() const
                {
                    return !fieldIds.empty();
                }

                /**
                 * Get field map.
                 *
                 * @return Fields.
                 */
                const FieldMap& GetFieldMap() const
                {
                    return fields;
                }

                /**
                 * Add field meta.
                 *
                 * @param fieldId Field ID.
                 * @param fieldName Field name.
                 * @param fieldTypeId Field type ID.
                 */
                IGNITE_IMPORT_EXPORT void AddField(int32_t fieldId, const std::string& fieldName, int32_t fieldTypeId);

                /**
                 * Copy fields from another snapshot.
                 *
                 * @param another Another instance.
                 */
                void CopyFieldsFrom(const BinaryTypeSnapshot* another);

                /**
                 * Get field ID.
                 *
                 * @param fieldName Field name.
                 * @return Field ID on success and 0 on fail. 
                 */
                int32_t GetFieldId(const std::string& fieldName)
                {
                    const FieldMap::const_iterator it = fields.find(fieldName);

                    return it == fields.end() ? 0 : it->second.GetFieldId();
                }

            private:
                /** Type name. */
                std::string typeName;

                /** Affinity field name. */
                std::string affFieldName;

                /** Type ID. */
                int32_t typeId;

                /** Known field IDs. */
                FieldIdSet fieldIds;

                /** Fields metadata. */
                FieldMap fields;
            };

            typedef BinaryTypeSnapshot Snap;
            typedef ignite::common::concurrent::SharedPointer<Snap> SPSnap;
        }
    }
}

#endif //_IGNITE_IMPL_BINARY_BINARY_TYPE_SNAPSHOT
