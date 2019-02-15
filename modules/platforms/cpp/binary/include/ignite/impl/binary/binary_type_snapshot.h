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
                 * @param typeId Type ID.
                 */
                BinaryTypeSnapshot(std::string typeName, int32_t typeId);

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
                void AddField(int32_t fieldId, const std::string& fieldName, int32_t fieldTypeId);

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