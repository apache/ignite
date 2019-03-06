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

#ifndef _IGNITE_IMPL_BINARY_BINARY_ID_RESOLVER
#define _IGNITE_IMPL_BINARY_BINARY_ID_RESOLVER

#include <map>

#include "ignite/common/concurrent.h"
#include "ignite/binary/binary_type.h"
#include "ignite/impl/binary/binary_type_handler.h"

namespace ignite
{
    namespace impl
    {
        namespace binary
        {
            /**
             * Binary type id resolver.
             */
            class BinaryIdResolver
            {
            public:
                /**
                 * Destructor.
                 */
                virtual ~BinaryIdResolver()
                {
                    // No-op.
                }

                /**
                 * Get binary object type ID.
                 *
                 * @return Type ID.
                 */
                virtual int32_t GetTypeId() = 0;

                /**
                 * Get binary object field ID.
                 *
                 * @param typeId Type ID.
                 * @param name Field name.
                 * @return Field ID.
                 */
                virtual int32_t GetFieldId(const int32_t typeId, const char* name) = 0;

                /**
                 * Get copy of the instance.
                 *
                 * @return Copy of the instance.
                 */
                virtual BinaryIdResolver* Clone() const = 0;
            };

            /**
             * Templated binary type resolver.
             */
            template<typename T>
            class TemplatedBinaryIdResolver : public BinaryIdResolver
            {
            public:
                /**
                 * Constructor.
                 */
                TemplatedBinaryIdResolver()
                {
                    // No-op.
                }

                virtual int32_t GetTypeId()
                {
                    return ignite::binary::BinaryType<T>::GetTypeId();
                }

                virtual int32_t GetFieldId(const int32_t typeId, const char* name)
                {
                    if (name)
                        return ignite::binary::BinaryType<T>::GetFieldId(name);

                    IGNITE_ERROR_FORMATTED_1(IgniteError::IGNITE_ERR_BINARY,
                        "Field name cannot be NULL.", "typeId", typeId);
                }

                virtual BinaryIdResolver* Clone() const
                {
                    return new TemplatedBinaryIdResolver<T>(*this);
                }
            };

            /**
             * Metadata binary type resolver.
             */
            class MetadataBinaryIdResolver : public BinaryIdResolver
            {
            public:
                /**
                 * Constructor.
                 */
                MetadataBinaryIdResolver() :
                    meta()
                {
                    // No-op.
                }

                /**
                 * Constructor.
                 *
                 * @param meta Binary type metadata snapshot.
                 */
                MetadataBinaryIdResolver(SPSnap meta) :
                    meta(meta)
                {
                    // No-op.
                }

                virtual int32_t GetTypeId()
                {
                    return meta.Get()->GetTypeId();
                }

                virtual int32_t GetFieldId(const int32_t typeId, const char* name)
                {
                    if (!name)
                    {
                        IGNITE_ERROR_FORMATTED_1(IgniteError::IGNITE_ERR_BINARY,
                            "Field name cannot be NULL.", "typeId", typeId);
                    }

                    int32_t res = meta.Get()->GetFieldId(name);

                    if (res == 0)
                        res = ignite::binary::GetBinaryStringHashCode(name);

                    if (res == 0)
                    {
                        IGNITE_ERROR_FORMATTED_2(IgniteError::IGNITE_ERR_BINARY,
                            "Field ID for the field name is zero. Please, redefine GetFieldId()"
                            " method for the type or change field name", "typeId", typeId, "fieldName", name);
                    }

                    return res;
                }

                virtual BinaryIdResolver* Clone() const
                {
                    return new MetadataBinaryIdResolver(*this);
                }

            private:
                /** Metadata snapshot. */
                SPSnap meta;
            };
        }
    }
}

#endif //_IGNITE_IMPL_BINARY_BINARY_ID_RESOLVER
